import os
import sys
import json
import time
import string
import asyncio
import logging
import re, shlex
import shutil
import socket
import subprocess
import threading
import signal
import uuid
import random
import fcntl
import platform
import psutil
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, date, timedelta


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "log")
os.makedirs(LOG_DIR, exist_ok=True)

# 메인 시스템 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "main_system.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("EncoderCore")

# [추가] 마지막 encoders-container(서버 정보 카드)에 실시간으로 뿌려줄 로그 버퍼 (최근 2000줄, 기존 500줄의 4배)
SERVER_LOG_BUFFER: deque = deque(maxlen=2000)


class BufferLogHandler(logging.Handler):
    def emit(self, record):
        SERVER_LOG_BUFFER.append(self.format(record))


_buffer_handler = BufferLogHandler()
_buffer_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(_buffer_handler)


# [추가] os.path.getsize/getctime/exists 등은 로컬 디스크라도 디스크가 바쁠 때(여러 채널이
# 동시에 쓰기/전송 중일 때) 순간적으로 지연될 수 있다. watchdog/transfer 루프에서 직접 호출하면
# 그 지연 동안 이벤트루프 전체가 블로킹되므로, 전부 스레드로 오프로드하는 얇은 래퍼를 통해서만 쓴다.
PIPE_BUFFER_SIZE = 512 * 1024  # 512KB

# [추가] bmxtranswrap 기반 새 transfer 방식에서 쓰는 growing-file 재시도 옵션값.
# main.py는 bmxtranswrap 프로세스의 시작/종료만 관리하고, 읽기 재시도는 이 값들을 통해
# bmxtranswrap 자신에게 전부 위임한다 (main.py는 재시도에 관여하지 않음).
DEFAULT_RETRY = 3
RETRY_DELAY = 5

# [추가] 디스크 용량 안전장치. rec_path(녹화 대상, 보통 로컬 RAID)는 다 차면 ffmpeg가 그 자리에서
# 에러로 죽으므로(녹화 파일 자체가 깨질 위험), 여유 있게 미리 감지해 STOP 버튼을 누른 것처럼
# 정지시킨다. file_target_root(전송 대상, 보통 NFS)는 그보다 넉넉한 기준으로, 용량 부족이 예상되면
# 아예 전송을 시작하지 않는다(녹화 자체엔 영향 없음).
MIN_REC_PATH_FREE_GB = 30
MIN_TARGET_FREE_GB = 100
DISK_CHECK_INTERVAL_SEC = 30  # rec_path 용량 확인 주기 (매 watchdog tick마다 하기엔 너무 잦음)


def enlarge_process_pipe_buffers(process: "asyncio.subprocess.Process", name: str, size: int = PIPE_BUFFER_SIZE):
    """[추가] stdout/stderr 파이프의 커널 버퍼 크기를 키운다 (기본은 보통 64KB).
    ffmpeg가 -progress/stderr로 짧은 순간 로그를 몰아서 쏟아낼 때(에러 버스트, 세그먼트 롤오버 등)
    파이프가 가득 차면 ffmpeg의 write()가 막혀버릴 수 있는데, 그러면 워치독이 progress 정지로
    오인해 불필요한 자동재시작을 유발할 수 있다. 버퍼를 넉넉히 키워 그 여유를 확보한다.
    실패해도(권한/커널 제한 등) 치명적이지 않으므로 경고 로그만 남기고 넘어간다."""
    for fd_num, label in ((1, "stdout"), (2, "stderr")):
        try:
            pipe_transport = process._transport.get_pipe_transport(fd_num)
            pipe_obj = pipe_transport.get_extra_info("pipe") if pipe_transport else None
            if pipe_obj is None:
                continue
            fcntl.fcntl(pipe_obj.fileno(), fcntl.F_SETPIPE_SZ, size)
        except Exception as e:
            logger.warning(f"[{name}] {label} 파이프 버퍼 크기 조정 실패: {e}")


# 파일명으로 사용할 수 없는 문자를 제거/치환하는 안전 규칙
_FILENAME_UNSAFE_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def sanitize_filename_tag(tag: str) -> str:
    if not tag:
        return ""
    tag = tag.strip()
    tag = _FILENAME_UNSAFE_RE.sub("_", tag)
    tag = tag.strip(" ._")
    return tag[:50]


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_all_ipv4_addresses() -> List[str]:
    # 여러 NIC이 존재하는 경우 모든 IPv4 주소를 나열
    ips = []
    try:
        for addrs in psutil.net_if_addrs().values():
            for addr in addrs:
                if addr.family == socket.AF_INET and addr.address not in ips:
                    ips.append(addr.address)
    except Exception:
        pass
    return ips if ips else [get_local_ip()]


def get_cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or "Unknown"


CONFIG_PATH: str = ""  # lifespan 시작 시 실제 config 파일 경로로 설정됨

# [추가] transfer 방식 선택. config.json의 전역 설정 "bmxtranswrap"(기본값 false)으로만 결정되며,
# 채널별 개별 설정은 불가능하다 - 모든 인코더가 이 값 하나를 공유한다.
# false(기본값): 기존 binary copy + footer/header update 방식(_transfer_manager_loop)
# true          : bmxtranswrap 외부 바이너리 기반 방식(_bmx_transfer_manager_loop)
BMXTRANSWRAP_ENABLED: bool = False

# [추가] RTSP 미리보기 스트림 인코딩 설정 (config.json의 "stream_video_encode" 로 오버라이드 가능)
# 이전에는 main.py에 libx264 옵션이 하드코딩되어 있었음. NVIDIA GPU가 설치된 시스템에서는
# 서버 기동 시 h264_nvenc 실동작 여부를 검사해 자동으로 NVENC로 전환한다.
NVENC_AVAILABLE: bool = False
STREAM_ENCODE_CFG: dict = {}
DEFAULT_STREAM_ENCODE_CFG = {
    "prefer_nvenc": True,
    # GPU 종류에 따라 동시 NVENC 세션 수가 제한될 수 있음 (예: GeForce 계열은 보통 3세션 제한,
    # Quadro/데이터센터 GPU는 사실상 무제한). 안전한 기본값으로 3을 사용하고, 실제 설치된
    # GPU에 맞게 config.json에서 조정하도록 한다.
    "nvenc_max_sessions": 3,
    "libx264": {"preset": "ultrafast", "tune": "zerolatency", "g": 30, "bf": 0, "pix_fmt": "yuv420p"},
    "h264_nvenc": {"preset": "p1", "tune": "ll", "g": 30, "bf": 0, "pix_fmt": "nv12"},
}


def detect_nvenc_available() -> bool:
    """h264_nvenc 인코더가 실제로 동작하는지 짧은 더미 인코딩으로 확인한다 (NVIDIA GPU/드라이버 유무 판별).
    nvidia-smi 등 별도 툴 설치 여부와 무관하게, ffmpeg가 실제로 NVENC를 호출할 수 있는지를 직접 검증한다.
    [수정] 테스트 해상도를 64x64 -> 256x256 으로 상향. NVENC H.264 인코더는 최소 프레임 크기 제약이 있어
    (예: "Frame Dimension less than the minimum supported value"), 64x64는 GPU/드라이버가 정상이어도
    항상 InitializeEncoder 실패로 이어져 정상 시스템을 NVENC 불가로 오판하는 버그가 있었음."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
             "-f", "lavfi", "-i", "color=black:size=256x256:rate=1",
             "-frames:v", "1", "-c:v", "h264_nvenc", "-f", "null", "-"],
            capture_output=True, timeout=10
        )
        if result.returncode != 0:
            # [추가] 실패 원인을 로그에 남겨서, 실제로 GPU가 없는 건지 파라미터/드라이버 문제인지
            # 운영자가 이 함수를 수동으로 재현하지 않고도 기동 로그만으로 바로 알 수 있게 한다.
            stderr_tail = result.stderr.decode("utf-8", errors="ignore").strip().splitlines()[-3:]
            logger.warning(f"NVENC 감지 실패 (returncode={result.returncode}): {' | '.join(stderr_tail)}")
        return result.returncode == 0
    except Exception:
        return False


def get_stream_video_encoder(enc: "FFmpegEncoderWrapper") -> tuple:
    """RTSP 미리보기 스트림에 사용할 (코덱명, 인코딩 옵션 dict) 를 반환한다.
    - 인코더별 config.json의 "prefer_nvenc" 값이 있으면 그걸 우선 사용하고, 없으면(null/미지정)
      전역 stream_video_encode.prefer_nvenc 기본값을 따른다.
    - GPU 종류에 따라 동시 NVENC 세션 수가 제한될 수 있어(nvenc_max_sessions), 이미 다른 인코더들이
      한도만큼 NVENC를 쓰고 있으면 이 인코더는 자동으로 libx264(SW)로 폴백한다."""
    prefer_nvenc = enc.cfg.prefer_nvenc
    if prefer_nvenc is None:
        prefer_nvenc = STREAM_ENCODE_CFG.get("prefer_nvenc", True)

    if NVENC_AVAILABLE and prefer_nvenc:
        max_sessions = STREAM_ENCODE_CFG.get("nvenc_max_sessions", 3)
        active_nvenc = sum(
            1 for other in encoders.values()
            if other is not enc
            and other.process is not None and other.process.returncode is None
            and other.active_stream_codec == "h264_nvenc"
        )
        if active_nvenc < max_sessions:
            enc.active_stream_codec = "h264_nvenc"
            return "h264_nvenc", STREAM_ENCODE_CFG["h264_nvenc"]
        logger.warning(
            f"[{enc.cfg.name}] NVENC 동시 세션 한도({max_sessions}) 도달 (현재 사용 중 {active_nvenc}개) "
            f"-> libx264(SW)로 폴백합니다."
        )

    enc.active_stream_codec = "libx264"
    return "libx264", STREAM_ENCODE_CFG["libx264"]


# [추가] config.json은 인코더 5개가 공유하는 하나의 파일이라, "REC ALL"처럼 여러 인코더의
# save_rec_tag_to_config()가 동시에 실행되면 각자 파일 전체를 읽고-수정하고-쓰는 과정이 겹쳐
# 서로의 변경사항을 덮어쓸 수 있다(lost update). 이 락으로 파일 read-modify-write를 직렬화한다.
CONFIG_SAVE_LOCK = asyncio.Lock()


async def save_encoder_fields_to_config(enc_id: int, fields: dict):
    """REC 버튼 클릭 시점의 값(파일명 태그, TRANSFER 체크박스 상태 등)을 config.json에 저장해,
    다음 서버 기동/UI 새로고침 시 복원할 수 있게 한다. fields는 {"rec_tag": ..., "transfer_enabled": ...}
    처럼 해당 인코더 항목에 병합할 키/값들이다."""
    if not CONFIG_PATH:
        return

    def _do_save():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            conf_data = json.load(f)

        changed = False
        for item in conf_data.get("encoders", []):
            if item.get("id") == enc_id:
                item.update(fields)
                changed = True
                break

        if changed:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(conf_data, f, ensure_ascii=False, indent=2)

    try:
        async with CONFIG_SAVE_LOCK:
            # 로컬 파일이지만, 락을 쥔 채로 이벤트루프를 블로킹하지 않도록 스레드에서 실행
            await asyncio.to_thread(_do_save)
    except Exception as e:
        logger.error(f"config.json 저장 실패 (enc_id={enc_id}, fields={fields}): {e}")


def get_main_py_version() -> str:
    """main.py 파일의 마지막 수정 시각을 'YYYY/MM/DD HH:MM' 형식으로 반환한다.
    화면 상단에 지금 서버가 실제로 돌리고 있는 코드가 언제 배포된 건지 표시하기 위함."""
    try:
        mtime = os.path.getmtime(os.path.join(BASE_DIR, "main.py"))
        return datetime.fromtimestamp(mtime).strftime("%Y/%m/%d %H:%M")
    except Exception:
        return "-"


def get_server_info() -> dict:
    mem = psutil.virtual_memory()
    return {
        "hostname": socket.gethostname(),
        "ip_list": get_all_ipv4_addresses(),
        "cpu_percent": psutil.cpu_percent(interval=None),
        "mem_percent": mem.percent,
        "cpu_model": get_cpu_model(),
        "cpu_cores": psutil.cpu_count(logical=True),
        "main_py_version": get_main_py_version(),  # [추가] main.py 마지막 수정일시 (버전 표시용)
        "log_lines": list(SERVER_LOG_BUFFER),  # 최근 2000줄 (SERVER_LOG_BUFFER maxlen으로 제한됨)
        # [추가] 스케줄러가 실제로 기준으로 삼는 "서버 시각"을 화면에 보여주기 위함.
        # 브라우저 로컬 시계 대신 서버 시계를 그대로 보여줘야 스케줄 시각과 비교했을 때 혼동이 없다.
        "server_time": datetime.now().strftime("%H:%M:%S %Y/%m/%d"),
        # [추가] config.json의 전역 "bmxtranswrap" 설정으로 결정되는 transfer 방식을 화면에 표시
        "transfer_mode": "bmxtranswrap" if BMXTRANSWRAP_ENABLED else "binary copy",
    }


@dataclass
class EncoderConfig:
    id: int
    name: str
    input_params: str
    segment_time: int
    rec_path: str
    stream_target_root: str
    file_target_root: str
    stderr_timeout_sec: float
    auto_restart_hold_time: float
    ffmpeg_custom_params: str
    rec_tag: str = ""  # config.json에 저장된, 마지막으로 사용한 녹화 파일명 태그
    # [추가] 인코더별 NVENC 사용 여부 오버라이드. None이면 전역 stream_video_encode.prefer_nvenc를 따름
    prefer_nvenc: Optional[bool] = None
    # [추가] SIGTERM 전송 후 SIGKILL로 강제 종료하기까지 대기하는 시간(초).
    # 녹화 파일이 커질수록(특히 대용량 MXF) footer/index 기록에 시간이 걸려 5초로는 부족할 수 있음.
    # 너무 짧으면 대용량 파일에서 SIGKILL로 MXF가 깨지고, 너무 길면 ffmpeg가 진짜로 멈췄을 때
    # STOP/재시작이 그만큼 오래 걸리므로 적당한 여유값(기본 20초)을 둔다.
    graceful_stop_timeout_sec: float = 20.0
    # [추가] Transfer 워커가 녹화 파일의 growing이 멈췄다고 판단한 뒤, header/footer 갱신을
    # 시작하기 전까지 한 번 더 대기하는 시간(초). 세그먼트 롤오버/파일 시스템 flush 타이밍의
    # 여유를 확보하기 위함 (기존 1초는 너무 짧아 20초로 확대).
    footer_update_delay_sec: float = 20.0
    # [추가] "녹화 파일 크기 증가 정지" 감시 타임아웃(초). 예전엔 8회(0.5초 간격, 총 4초) 고정이었는데,
    # segment_time을 3600으로 키운 뒤 세그먼트 롤오버 순간 ffmpeg가 잠깐 버벅이며(footer/index 기록
    # 부담 증가) 오탐 자동재시작이 발생하는 걸 확인해 여유값을 늘렸다.
    file_stagnation_timeout_sec: float = 8.0
    # [추가] 세그먼트 파일이 새로 바뀐(롤오버) 직후 이 시간(초) 동안은 stderr progress/파일 크기
    # 증가 감시 타임아웃을 3배로 완화한다. 큰 세그먼트(특히 segment_time이 길수록)를 닫고 새로
    # 여는 순간 ffmpeg/DeckLink 캡처가 잠깐 stall되며 프레임이 드랍되는 걸 실측으로 확인했음.
    segment_rollover_grace_sec: float = 15.0
    # [추가] REC 버튼 클릭 시점의 TRANSFER 체크박스 상태를 저장해두는 필드. 서버 기동 시 이 값으로
    # 인코더의 초기 transfer_enabled를 복원해, 재시작 후에도 마지막으로 쓰던 설정이 유지되게 한다.
    transfer_enabled: bool = False
    # [추가] 사용자가 REC 버튼으로 수동 녹화를 시작하면 True, STOP을 누르거나 STREAM으로 전환하면
    # False로 저장된다. main.py가 재시작(수동 재시작, 크래시 후 systemd 자동 재시작, /debug의
    # 재시작 버튼 등 무엇이든)되면, 서버 기동 시 이 값이 True인 채널은 자동으로 REC를 다시
    # 시작시켜 수동 녹화가 끊기지 않게 한다. 예약 스케줄러가 시작한 녹화는 이 값과 무관하게
    # tick()이 자체적으로 재개하므로 건드리지 않는다.
    manual_recording_active: bool = False
    # [추가] REC 버튼 클릭 시점의 TEST 체크박스 상태를 저장해두는 필드. 이게 없으면 TEST 모드로
    # 녹화 중이던 채널이 예기치 않게 재시작(자동 업데이트에 의한 systemd 재시작 등)된 뒤,
    # manual_recording_active 자동 재개 로직이 test_enabled를 모른 채 기본값(False)으로 다시
    # 시작시켜 "TEST 모드인 줄 알았는데 실제 DeckLink 입력으로 녹화되고 있었다"는 사고가 날 수
    # 있다(실사고로 발견됨). transfer_enabled와 동일한 방식으로 저장/복원한다.
    test_enabled: bool = False


class FFmpegEncoderWrapper:
    def __init__(self, cfg: EncoderConfig, status_callback):
        self.cfg = cfg
        self.status_cb = status_callback
        self.state = "free"
        self.process: Optional[asyncio.subprocess.Process] = None
        self.active_stream_codec: Optional[str] = None  # 현재 세션에서 실제로 선택된 스트림 비디오 코덱 (nvenc 세션 카운트용)
        # [추가] start()/stop()/자동재시작이 같은 인코더에 대해 동시에 실행되며 self.process를
        # 놓고 경합하던 문제(중복 클릭 시 프로세스 고아화, DeckLink 장치 중복 오픈)를 막기 위한 락.
        self._lifecycle_lock = asyncio.Lock()
        self.user_stopped = False
        # [수정] config.json에 저장된 마지막 TRANSFER 체크박스 상태로 초기화 (서버 재시작 후 복원용)
        self.transfer_enabled = cfg.transfer_enabled
        self.transfer_manager_task = None
        self.transfer_workers = set()
        self.transfer_stats = {}  # 추가: 각 파일의 복사 진행 상태를 담을 딕셔너리
        self.is_restarting = False  # <--- 이 줄을 추가합니다.
        self.transfer_handled_files = set()
        # [추가] bmxtranswrap 기반 새 transfer 방식 전용 상태. 기존 transfer_* 상태와는 별개로
        # 관리한다 (기존 _transfer_manager_loop/_transfer_worker는 코드에 그대로 남겨두고, 실제
        # 실행은 이 새 경로로 대체하는 구조 - 문제가 생기면 start()에서 호출을 되돌리기만 하면 됨).
        self.bmx_transfer_manager_task = None
        self.bmx_transfer_handled_files = set()
        self.bmx_transfer_launch_tasks = set()   # 스태거 대기 중인 asyncio.Task들
        self.bmx_transfer_threads = {}           # src_file -> threading.Thread
        self.bmx_transfer_stop_events = {}        # src_file -> threading.Event (graceful shutdown 신호용)
        self.is_shutting_down = False  # <--- 이 줄을 추가합니다.
        self.transfer_aborted = False  # <--- 이 줄을 추가합니다.
        # [수정] config.json에 저장된 마지막 TEST 체크박스 상태로 초기화 (transfer_enabled와 동일한
        # 이유 - 서버가 예기치 않게 재시작돼도 manual_recording_active 자동 재개 로직이 TEST 모드를
        # 알 수 있어야, 진짜 DeckLink 입력으로 잘못 재개되는 사고를 막을 수 있다).
        self.test_enabled = cfg.test_enabled
        self.rec_tag = sanitize_filename_tag(cfg.rec_tag)  # 녹화 파일명에 덧붙일 사용자 지정 태그(텍스트 박스 입력값), config.json에서 복원
        # [추가] 지금 도는 rec+stream 세션이 스케줄러가 시작한 것인지(그 스케줄 id) 아니면 사용자가
        # 수동으로 REC를 누른 것인지(None) 구분한다. 스케줄러는 이 값을 보고: 1) 수동 녹화 중인 채널은
        # 예약 시각이 와도 건너뛰고, 2) 자기가 시작한 세션만 자기 종료 시각에 정지시키며 다른 스케줄/
        # 수동 녹화를 실수로 건드리지 않는다. start()/자동재시작은 이 값을 건드리지 않아, 세그먼트
        # 롤오버 등으로 인한 auto-restart 중에도 "이 녹화는 스케줄 X 소유"라는 정보가 유지된다.
        self.started_by_schedule: Optional[str] = None

        # 텔레메트리 데이터
        self.progress_time = "00:00:00.00"
        self.speed = "0x"
        self.file_size = 0
        self.fflog_size = 0
        
        # 워치독 상태 변수
        self.last_progress_ts = time.time()
        # [추가] ffmpeg 최초 기동 시에는 DeckLink 초기화/필터그래프 구성 등으로 첫 progress
        # 출력까지 평소보다 오래 걸릴 수 있어, 최초 1회에 한해 워치독 타임아웃을 3배로 완화한다.
        self.has_received_first_progress = False
        self.error_burst_window: List[float] = []
        self.last_file_size = -1
        # [수정] 워치독 폴링 간격이 0.5~1초 사이 랜덤이라 "횟수 x 0.5초" 식으로는 실제 경과
        # 시간을 정확히 못 재서, 크기가 안 바뀌기 시작한 실제 시각(timestamp)을 기록하는 방식으로 변경.
        self.file_stagnant_since_ts: Optional[float] = None
        # [추가] 세그먼트 롤오버 감지 + 그 직후 워치독 유예에 사용
        self.last_seen_rec_file: Optional[str] = None
        self.last_rollover_ts = 0.0
        # [추가] rec_path 남은 용량 체크 쓰로틀링용 (DISK_CHECK_INTERVAL_SEC마다 한 번만 확인)
        self.last_disk_space_check_ts = 0.0
        self.current_rec_file = ""
        self.ffreport_path = ""
        # [추가] 이번 REC 세션이 시작된 시각. transfer 대상 파일이 "이번 세션에서 만든 파일"인지,
        # 서버 재시작 등으로 남아있는 "이전 세션의 leftover 파일"인지 구분하는 데 사용한다.
        self.session_start_ts = 0.0
        
        self.monitor_task: Optional[asyncio.Task] = None
        self.pipe_reader_task: Optional[asyncio.Task] = None
        # [추가] FFmpeg 로그 최근 200줄 저장소
        self.ffmpeg_logs = deque(maxlen=200)

        self.monitor_task: Optional[asyncio.Task] = None
        self.pipe_reader_task: Optional[asyncio.Task] = None

        # [추가] asyncio.to_thread()는 내부적으로 전역 공유 ThreadPoolExecutor(기본 min(32, cpu+4))를
        # 쓰는데, 이걸 5개 인코더가 전부 같이 쓴다. file_target_root가 NFS(hard mount)라서 서버 장애/
        # 네트워크 단절 시 open()/read()/write()가 타임아웃 없이 영원히 멈출 수 있고, 그러면 그 스레드는
        # 프로세스가 죽을 때까지 회수가 안 된다. 한 채널의 NFS 장애가 전역 풀의 스레드를 하나씩 파먹어
        # 결국 다른 채널의 (config 저장 제외) 파일시스템 작업까지 굶주리게 만들 수 있어, 채널마다
        # 전용의 작은 스레드풀을 둬서 장애를 그 채널 안으로 격리한다.
        self._io_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix=f"io-{cfg.name}")

    async def _run_io(self, func, *args):
        """이 인코더 전용 스레드풀에서 블로킹 파일시스템 호출을 실행한다.
        asyncio.to_thread()(전역 공유 풀) 대신 쓰는 이유는 __init__의 self._io_executor 주석 참고."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._io_executor, func, *args)

    def _build_command(self) -> List[str]:
        n = self.cfg.id
        v_url = f"{self.cfg.stream_target_root}/input{n}00"
        a12_url = f"{self.cfg.stream_target_root}/input{n}12"
        a34_url = f"{self.cfg.stream_target_root}/input{n}34"
        a56_url = f"{self.cfg.stream_target_root}/input{n}56"
        a78_url = f"{self.cfg.stream_target_root}/input{n}78"

        # 대상 rec_path 가 존재하지 않는 경우 폴더 자동 생성
        os.makedirs(self.cfg.rec_path, exist_ok=True)
        # 텍스트 박스로 입력한 태그는 파일명 헤더(맨 앞)로 붙인다
        tag_prefix = f"{self.rec_tag}_" if self.rec_tag else ""
        rec_pattern = os.path.join(self.cfg.rec_path, f"{tag_prefix}{self.cfg.name}_%Y%m%d_%H%M%S.mxf")
        self.current_rec_file = rec_pattern

        # ---------------------------------------------------------
        # 1. Preset 키 생성 및 동적 입력값 정의
        # ---------------------------------------------------------
        source_type = "test" if self.test_enabled else "decklink"
        preset_key = f"{source_type}-{self.state}"  # 예: "test-stream" 또는 "decklink-rec+stream"
        
        test_input_args = [
            "-f", "lavfi", "-re", "-i", "testsrc2=size=1920x1080:rate=60000/1001",
            "-f", "lavfi", "-re", "-i", "sine=frequency=1000:sample_rate=48000:beep_factor=4"
        ]
        decklink_input_args = shlex.split(self.cfg.input_params)

        # ---------------------------------------------------------
        # 2. Preset 딕셔너리 정의 (각 상황별 필터와 맵핑 값을 미리 세팅)
        # ---------------------------------------------------------
        presets = {
            "test-stream": {
                "input_args": test_input_args,
                "filter_complex": (
                    "[0:v]fps=60000/1001,tinterlace=4,scale=-2:400[v_stream];"
                    "[1:a]asplit=4[as1][as2][as3][as4];"
                    "[as1]pan=stereo|c0=c0|c1=c0[a12];"
                    "[as2]pan=stereo|c0=c0|c1=c0[a34];"
                    "[as3]pan=stereo|c0=c0|c1=c0[a56];"
                    "[as4]pan=stereo|c0=c0|c1=c0[a78]"
                ),
                "v_map_stream": "[v_stream]",
                "v_map_rec": None
            },
            "decklink-stream": {
                "input_args": decklink_input_args,
                "filter_complex": (
                    "[0:v]scale=-2:400[v_stream];"
                    "[0:a]asplit=4[as1][as2][as3][as4];"
                    "[as1]pan=stereo|c0=c0|c1=c1[a12];"
                    "[as2]pan=stereo|c0=c2|c1=c3[a34];"
                    "[as3]pan=stereo|c0=c4|c1=c5[a56];"
                    "[as4]pan=stereo|c0=c6|c1=c7[a78]"
                ),
                "v_map_stream": "[v_stream]",
                "v_map_rec": None
            },
            "test-rec+stream": {
                "input_args": test_input_args,
                "filter_complex": (
                    "[0:v]fps=60000/1001,tinterlace=4,split=2[v_split][v_rec];"
                    "[v_split]scale=-2:400[v_stream];"
                    "[1:a]asplit=12[as1][as2][as3][as4][ar0][ar1][ar2][ar3][ar4][ar5][ar6][ar7];"
                    "[as1]pan=stereo|c0=c0|c1=c0[a12];"
                    "[as2]pan=stereo|c0=c0|c1=c0[a34];"
                    "[as3]pan=stereo|c0=c0|c1=c0[a56];"
                    "[as4]pan=stereo|c0=c0|c1=c0[a78];"
                    "[ar0]pan=1c|c0=c0[a_ch0];[ar1]pan=1c|c0=c0[a_ch1];"
                    "[ar2]pan=1c|c0=c0[a_ch2];[ar3]pan=1c|c0=c0[a_ch3];"
                    "[ar4]pan=1c|c0=c0[a_ch4];[ar5]pan=1c|c0=c0[a_ch5];"
                    "[ar6]pan=1c|c0=c0[a_ch6];[ar7]pan=1c|c0=c0[a_ch7]"
                ),
                "v_map_stream": "[v_stream]",
                "v_map_rec": "[v_rec]"
            },
            "decklink-rec+stream": {
                "input_args": decklink_input_args,
                "filter_complex": (
                    "[0:v]split=2[v_split][v_rec];"
                    "[v_split]scale=-2:400[v_stream];"
                    "[0:a]asplit=12[as1][as2][as3][as4][ar0][ar1][ar2][ar3][ar4][ar5][ar6][ar7];"
                    "[as1]pan=stereo|c0=c0|c1=c1[a12];"
                    "[as2]pan=stereo|c0=c2|c1=c3[a34];"
                    "[as3]pan=stereo|c0=c4|c1=c5[a56];"
                    "[as4]pan=stereo|c0=c6|c1=c7[a78];"
                    "[ar0]pan=1c|c0=c0[a_ch0];[ar1]pan=1c|c0=c1[a_ch1];"
                    "[ar2]pan=1c|c0=c2[a_ch2];[ar3]pan=1c|c0=c3[a_ch3];"
                    "[ar4]pan=1c|c0=c4[a_ch4];[ar5]pan=1c|c0=c5[a_ch5];"
                    "[ar6]pan=1c|c0=c6[a_ch6];[ar7]pan=1c|c0=c7[a_ch7]"
                ),
                "v_map_stream": "[v_stream]",
                "v_map_rec": "[v_rec]"
            }
        }

        # 선택된 Preset 가져오기 (오타나 예외 상태를 방지하기 위해 fallback 처리 권장)
        if preset_key not in presets:
            raise ValueError(f"지원하지 않는 스트리밍/녹화 모드입니다: {preset_key}")
            
        current_preset = presets[preset_key]

        # ---------------------------------------------------------
        # 3. FFmpeg Command 조립
        # ---------------------------------------------------------
        cmd = [
            "ffmpeg", "-y", "-hide_banner",
            "-stats_period", "1", "-nostats",
            "-progress", "pipe:2",
        ] + current_preset["input_args"] + [
            "-filter_complex", current_preset["filter_complex"]
        ]

        # RTSP 스트리밍 출력 (공통)
        # [수정] libx264 하드코딩 제거 -> config.json(stream_video_encode)에서 읽어오며,
        # NVIDIA GPU가 있는 시스템에서는 h264_nvenc로 자동 전환된다.
        stream_codec, stream_opts = get_stream_video_encoder(self)
        cmd += [
            "-map", current_preset["v_map_stream"], "-c:v", stream_codec,
            "-preset:v", str(stream_opts["preset"]), "-bf", str(stream_opts["bf"]),
            "-g", str(stream_opts["g"]), "-tune", str(stream_opts["tune"]),
            "-pix_fmt", str(stream_opts["pix_fmt"]), "-f", "rtsp", v_url,
            "-map", "[a12]", "-c:a", "libopus", "-ac", "2", "-b:a", "32k", "-f", "rtsp", a12_url,
            "-map", "[a34]", "-c:a", "libopus", "-ac", "2", "-b:a", "32k", "-f", "rtsp", a34_url,
            "-map", "[a56]", "-c:a", "libopus", "-ac", "2", "-b:a", "32k", "-f", "rtsp", a56_url,
            "-map", "[a78]", "-c:a", "libopus", "-ac", "2", "-b:a", "32k", "-f", "rtsp", a78_url
        ]

        # XDCAM HD422 MXF (녹화 상태일 경우)
        if self.state == "rec+stream":
            template_ctx = {
                "v_url": v_url, "a12_url": a12_url, "a34_url": a34_url, "a56_url": a56_url, "a78_url": a78_url,
                "rec_pattern": rec_pattern, "seg_time": str(self.cfg.segment_time), "name": self.cfg.name
            }
            custom_params = string.Template(self.cfg.ffmpeg_custom_params).safe_substitute(template_ctx).split()
            current_tc = get_current_df_timecode()
            logger.info(f"[{self.cfg.name}] Start Timecode 적용: {current_tc}")
            
            cmd += [
                "-map", current_preset["v_map_rec"],
                "-map", "[a_ch0]", "-map", "[a_ch1]", "-map", "[a_ch2]", "-map", "[a_ch3]",
                "-map", "[a_ch4]", "-map", "[a_ch5]", "-map", "[a_ch6]", "-map", "[a_ch7]"
            ] + custom_params + [
                "-c:a", "pcm_s24le",
                "-timecode", current_tc,
                "-metadata:s:v:0", f"timecode={current_tc}",
                "-tag:d", "tmcd",
                "-f", "segment",
                "-segment_time", str(self.cfg.segment_time),
                "-segment_format", "mxf",
                "-strftime", "1",
                rec_pattern
            ]

        return cmd

    # [추가] rec+stream 시작 때마다 config.json에서 다시 불러와 반영하는 필드 목록.
    # main.py 실행 중에도 config.json이 직접 수정될 수 있으므로(운영자가 경로/파라미터 변경 등),
    # 매번 REC를 시작하는 시점의 최신 값을 쓰기 위함이다. 다른 필드(transfer_enabled, rec_tag 등)는
    # 이미 자체적인 저장/복원 경로(save_encoder_fields_to_config)가 있으므로 여기서 건드리지 않는다.
    LIVE_RELOAD_FIELDS = ("rec_path", "file_target_root", "segment_time", "ffmpeg_custom_params")

    async def _reload_live_config_fields(self):
        if not CONFIG_PATH:
            return

        def _do_read():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                conf_data = json.load(f)
            for item in conf_data.get("encoders", []):
                if item.get("id") == self.cfg.id:
                    return item
            return None

        try:
            item = await asyncio.to_thread(_do_read)
        except Exception as e:
            logger.error(f"[{self.cfg.name}] config.json 재로딩 실패, 기존 값 유지: {e}")
            return
        if item is None:
            return

        changes = []
        for field_name in self.LIVE_RELOAD_FIELDS:
            if field_name in item:
                old_value = getattr(self.cfg, field_name)
                new_value = item[field_name]
                if old_value != new_value:
                    changes.append(f"{field_name}: {old_value!r} -> {new_value!r}")
                setattr(self.cfg, field_name, new_value)
        if changes:
            logger.info(f"[{self.cfg.name}] REC 시작 - config.json에서 최신 값 재로딩: {', '.join(changes)}")

    async def start(self, target_state: str, transfer_enabled: bool = False, test_enabled: bool = False, rec_tag: str = "",
                     schedule_id: Optional[str] = None):
        # [추가] schedule_id: 이 세션이 스케줄러가 시작한 것이면 그 스케줄 id, 수동 시작이면 None.
        # 호출부(수동 REC/STREAM API, 스케줄러, 자동재시작)가 각자 알맞은 값을 넘긴다. 락 안에서
        # state 전환과 함께 원자적으로 반영해, "start()는 끝났는데 소유권 표시는 아직 안 됨" 같은
        # 레이스 윈도우가 생기지 않게 한다.
        async with self._lifecycle_lock:
            await self._stop_process()
            self.state = target_state
            self.transfer_enabled = transfer_enabled
            self.test_enabled = test_enabled  # <--- 새로 추가
            self.rec_tag = sanitize_filename_tag(rec_tag)
            self.started_by_schedule = schedule_id
            self.ffmpeg_logs.clear() # [추가] 시작할 때 이전 로그 초기화
            self.ffmpeg_logs.append("=== FFmpeg Process Started ===")
            self.user_stopped = False
            self.is_restarting = False  # <--- 이 줄을 추가합니다.
            self.transfer_aborted = False  # <--- 시작할 때마다 초기화되도록 추가합니다.
            self.last_progress_ts = time.time()
            self.has_received_first_progress = False  # 재시작할 때마다 "최초 기동 유예"를 다시 적용
            self.error_burst_window.clear()
            self.last_file_size = -1
            self.file_stagnant_since_ts = None
            # [추가] 재시작마다 롤오버 감지 상태 초기화. last_rollover_ts를 지금 시각으로 잡아두면
            # 기동 직후에도 has_received_first_progress 유예와 함께 자연스럽게 여유 구간이 생긴다.
            self.last_seen_rec_file = None
            self.last_rollover_ts = time.time()
            # [추가] 2초 여유를 둬서, 파일시스템 타임스탬프 해상도/클럭 오차로 인해 방금 시작한
            # 세그먼트 파일이 오탐으로 "이전 세션 파일" 취급되는 일이 없도록 한다.
            self.session_start_ts = time.time() - 2.0

            self.ffreport_path = os.path.join(LOG_DIR, f"ffreport_{self.cfg.name}_{int(time.time())}.log")
            env = os.environ.copy()
            env["FFREPORT"] = f"file={self.ffreport_path}:level=32"

            # [추가] rec+stream을 시작할 때마다 rec_path/file_target_root/segment_time/
            # ffmpeg_custom_params를 config.json에서 다시 읽어와 반영한다 (아래 명령 조립 전에
            # 반드시 선행돼야 함).
            if target_state == "rec+stream":
                await self._reload_live_config_fields()

            cmd = self._build_command()
            logger.info(f"[{self.cfg.name}] 실행 커맨드: {' '.join(cmd)}")

            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            # [추가] stdout/stderr 파이프 커널 버퍼를 512KB로 확장 (기본 파이프 사이즈로는
            # 로그가 몰릴 때 ffmpeg의 write()가 블로킹될 수 있음)
            enlarge_process_pipe_buffers(self.process, self.cfg.name)

            self.pipe_reader_task = asyncio.create_task(self._listen_pipes())
            self.monitor_task = asyncio.create_task(self._watchdog_loop())

            if self.state == "rec+stream" and self.transfer_enabled:
                # [수정] config.json의 전역 설정 "bmxtranswrap"(BMXTRANSWRAP_ENABLED)에 따라 두 방식 중
                # 하나를 선택한다. 채널별 개별 설정은 없다 - 서버 전체가 같은 방식을 쓴다.
                if BMXTRANSWRAP_ENABLED:
                    self.bmx_transfer_manager_task = asyncio.create_task(self._bmx_transfer_manager_loop())
                else:
                    self.transfer_manager_task = asyncio.create_task(self._transfer_manager_loop())

            await self.status_cb()

    async def _stop_process(self):
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
        if self.pipe_reader_task and not self.pipe_reader_task.done():
            self.pipe_reader_task.cancel()

        # [추가] 재시작 시에도 기존 매니저 루프를 확실하게 종료시켜 중복 실행 차단
        if getattr(self, "transfer_manager_task", None) and not self.transfer_manager_task.done():
            self.transfer_manager_task.cancel()

        # [추가] bmxtranswrap 기반 새 transfer 방식의 정리.
        # 매니저 루프(새 파일 감지 담당)와 스태거 대기 중인 launch task는 언제든 취소한다 - 이건
        # 그냥 "다음 파일을 또 새로 시작할지" 여부일 뿐, 이미 돌고 있는 bmxtranswrap 프로세스와는
        # 무관하다.
        if getattr(self, "bmx_transfer_manager_task", None) and not self.bmx_transfer_manager_task.done():
            self.bmx_transfer_manager_task.cancel()
        for task in list(getattr(self, "bmx_transfer_launch_tasks", set())):
            if not task.done():
                task.cancel()

        # [추가] 이미 돌고 있는 bmxtranswrap 프로세스에 SIGTERM을 보내는 건 main.py 자체가
        # 종료(서버 재시작/셧다운, self.is_shutting_down=True)될 때만 한다. STOP 버튼(수동 정지),
        # 자동재시작, REC 재시작 등 "녹화만 멈추고 main.py는 계속 도는" 경우에는 건드리지 않는다 -
        # 그 파일은 이미 녹화가 끝났으니(더 growing 안 함) bmxtranswrap이 자기 --gf-retries/
        # --gf-delay 안에서 알아서 따라잡고 정상 종료한다. 여기서 손대지 않으면 마지막 몇 초가
        # 잘려나가는 일도 없다.
        if self.is_shutting_down:
            for stop_event in list(getattr(self, "bmx_transfer_stop_events", {}).values()):
                stop_event.set()
            for src_file, thread in list(getattr(self, "bmx_transfer_threads", {}).items()):
                if thread.is_alive():
                    # join()은 블로킹 호출이라 이벤트 루프를 막지 않도록 채널 전용 스레드풀에서 대기
                    await self._run_io(thread.join, self.cfg.graceful_stop_timeout_sec + 10)
                    if thread.is_alive():
                        logger.warning(f"[{self.cfg.name}] bmxtranswrap 스레드가 시간 내에 끝나지 않음: {src_file}")
            self.bmx_transfer_threads.clear()
            self.bmx_transfer_stop_events.clear()

        if self.process and self.process.returncode is None:
            logger.info(f"[{self.cfg.name}] PID {self.process.pid} 로 SIGTERM 전송... "
                        f"(graceful_stop_timeout_sec={self.cfg.graceful_stop_timeout_sec}초)")
            try:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout=self.cfg.graceful_stop_timeout_sec)
                except asyncio.TimeoutError:
                    logger.warning(f"[{self.cfg.name}] Graceful 종료 시간 초과"
                                   f"({self.cfg.graceful_stop_timeout_sec}초). SIGKILL 전송.")
                    self.process.kill()
                    await self.process.wait()
            except ProcessLookupError:
                pass
        self.process = None
        self.active_stream_codec = None  # NVENC 세션 카운트에서 즉시 제외

    async def stop(self, force: bool = False):
            if force:
                self.is_shutting_down = True  # Ctrl-C 로 인한 강제 종료 시 플래그 ON
            # 0. 종료 전, 실제로 돌고 있던 프로세스가 있었는지 기억해둔다 (fflog tail 로깅 대상 판별용)
            had_process = self.process is not None and self.process.returncode is None
            # 1. 프로세스 종료 대기 전에 상태 변수부터 즉시 초기화 (HTTP 중단 방어)
            self.user_stopped = True
            self.state = "free"
            # [수정] 여기서 transfer_enabled를 False로 초기화하면, STOP을 누르는 순간 config.json에서
            # 복원됐거나 마지막 REC 때 체크했던 TRANSFER 상태가 지워져버려 화면 체크박스가 꺼진다.
            # STOP 버튼은 "지금 하던 걸 멈춘다"는 뜻이지 "TRANSFER 선호값을 리셋한다"는 뜻이 아니므로,
            # 건드리지 않고 그대로 둔다 (다음 REC 클릭 시점에만 실제로 갱신됨).
            self.progress_time = "00:00:00.00"
            self.speed = "0x"
            self.file_size = 0
            # [추가] STOP은 항상 "이 채널의 현재 세션은 끝났다"는 뜻이므로, 스케줄 소유권도 여기서 해제한다.
            self.started_by_schedule = None

            # 2. 바뀐 free 상태를 접속 중인 큐에 즉각 브로드캐스트 (락 밖에서, 최대한 빨리 UI에 반영)
            await self.status_cb()

            # 3. 실제 프로세스 강제 종료 로직 실행 (에러 발생 시 무시하고 넘어가도록 보호)
            # [수정] start()와 동일한 락을 사용해, 중복 STOP/REC 요청이 겹쳐도 self.process를 놓고
            # 경합하지 않도록 직렬화한다. (예전엔 이 부분에서 'NoneType' object has no attribute 'kill'
            # 이 나면서 실제 ffmpeg 프로세스를 고아로 남기는 버그가 있었음)
            #
            # [추가] 동시성 버그 수정: STOP 요청이 락을 기다리는 동안 다른 REC/STOP 요청이 먼저 락을
            # 잡고 새 프로세스를 띄워버릴 수 있다. 이 경우 이 STOP이 뒤늦게 락을 잡아 "그 새 프로세스"를
            # 죽이는 게 맞지만(=STOP의 본질은 "끝날 때 반드시 아무것도 안 돌고 있게 만든다"), state를
            # 락 밖(1번)에서만 free로 설정해두면 그 사이 다른 요청이 state를 다시 덮어써서 "실제로는 아무
            # 것도 안 도는데 화면엔 REC 중"이라는 불일치가 영구적으로 남을 수 있다 (실제 재현 확인됨).
            # 그래서 _stop_process() 직후, 락 안에서 상태를 다시 한 번 확정적으로 free로 맞춘다.
            try:
                async with self._lifecycle_lock:
                    await self._stop_process()
                    self.state = "free"
                    # [수정] 위 1번과 동일한 이유로 transfer_enabled는 여기서도 건드리지 않는다.
                    self.progress_time = "00:00:00.00"
                    self.speed = "0x"
                    self.file_size = 0
                    self.started_by_schedule = None
                    self.is_restarting = False
            except Exception as e:
                logger.error(f"[{self.cfg.name}] 프로세스 종료 중 에러 발생: {e}")

            # [추가] 락 안에서 확정한 최종 상태를 다시 한 번 브로드캐스트 (다른 요청과 경합해
            # 중간에 state가 바뀌었던 경우에도 UI가 최종적으로는 정확한 free 상태를 받도록 보장)
            await self.status_cb()

            # 4. STOP 버튼(또는 서버 종료)으로 실제 프로세스가 종료된 경우, 원인 파악용으로 fflog tail 10줄 로깅
            if had_process:
                self._log_fflog_tail("서버 종료로 인한 강제 정지" if force else "STOP 버튼에 의한 종료")

            # [수정] transfer_manager_task 취소는 _stop_process()가 락 안에서 이미 처리한다.
            # 여기서 락 밖에 있던 중복 취소 코드는 제거함 — 그 사이 다른 start()가 새로
            # transfer_manager_task를 만들었다면 엉뚱한(새) 태스크를 취소해버리는 레이스가 있었음.


    async def _listen_pipes(self):
        async def read_stderr():
            while self.process and self.process.returncode is None:
                line = await self.process.stderr.readline()
                if not line:
                    break
                decoded = line.decode('utf-8', errors='ignore').strip()
                # [추가] 덱에 로그 라인 저장
                self.ffmpeg_logs.append(f"[STDERR] {decoded}")
                if "=" in decoded:
                    k, v = decoded.split("=", 1)
                    if k == "out_time":
                        self.progress_time = v
                        self.last_progress_ts = time.time()
                        self.has_received_first_progress = True
                    elif k == "speed":
                        self.speed = v
                    #elif k == "total_size" and v.isdigit():
                    #    self.file_size = int(v)

        async def read_stdout():
            while self.process and self.process.returncode is None:
                line = await self.process.stdout.readline()
                if not line:
                    break
                decoded = line.decode('utf-8', errors='ignore')
                decodec_lower = decoded.lower()
                self.ffmpeg_logs.append(f"[STDOUT] {decoded}")
                if "error" in decodec_lower or "warning" in decodec_lower:
                    now = time.time()
                    self.error_burst_window.append(now)
                    # 1초 윈도우 내 에러/경고 로그 빈도 추적
                    self.error_burst_window = [t for t in self.error_burst_window if now - t <= 1.0]
                    if len(self.error_burst_window) >= 15:
                        logger.error(f"[{self.cfg.name}] Stdout 에러 버스트 감지 ({len(self.error_burst_window)} lines/sec)")
                        asyncio.create_task(self._trigger_auto_restart("Stdout Error Burst"))
                        break

        await asyncio.gather(read_stderr(), read_stdout(), return_exceptions=True)

    async def _watchdog_loop(self):
        while True:
            # [수정] 5채널이 전부 정확히 0.5초 주기로 맞물려 있으면 os.path.getsize/listdir 같은
            # 디스크 조회가 순간적으로 몰릴 수 있어, 0.5~1.0초 사이 랜덤 간격으로 서로 어긋나게 한다.
            await asyncio.sleep(random.uniform(0.5, 1.0))
            if self.user_stopped or not self.process:
                break

            # 1. 프로세스 자연 중단 감시
            if self.process.returncode is not None:
                # 변경: await -> asyncio.create_task
                asyncio.create_task(self._trigger_auto_restart(f"Process terminated (code: {self.process.returncode})"))
                break

            now = time.time()

            # [추가] 세그먼트 롤오버 감지 (rec+stream일 때만 의미 있음). segment_time이 클수록
            # (예: 3600초) 세그먼트를 닫고 새로 여는 순간 footer/index 기록 부담이 커져서 ffmpeg/
            # DeckLink 캡처가 잠깐 stall되며 프레임 드랍이 발생하는 걸 실측으로 확인했다. 새 세그먼트
            # 파일이 감지되면 last_rollover_ts를 갱신해 그 직후 일정 시간 워치독을 완화한다.
            latest_file = None
            if self.state == "rec+stream":
                # [수정] os.listdir/os.path.getctime을 스레드로 오프로드 (디스크 바쁠 때 이벤트루프 보호)
                latest_file = await self._get_latest_rec_file()
                if latest_file and latest_file != self.last_seen_rec_file:
                    self.last_seen_rec_file = latest_file
                    self.last_rollover_ts = now

            in_rollover_grace = (now - self.last_rollover_ts) < self.cfg.segment_rollover_grace_sec
            in_grace = (not self.has_received_first_progress) or in_rollover_grace
            grace_multiplier = 3 if in_grace else 1

            # 2. progress 갱신 타임아웃 감시
            # [수정] 최초 기동 직후, 또는 세그먼트 롤오버 직후(segment_rollover_grace_sec 이내)에는
            # 평소 감시 시간의 3배까지 유예한다. 그 외에는 원래 stderr_timeout_sec 기준.
            effective_timeout = self.cfg.stderr_timeout_sec * grace_multiplier
            if now - self.last_progress_ts > effective_timeout:
                # 변경: await -> asyncio.create_task
                if not self.has_received_first_progress:
                    reason = "Initial Stderr Progress Timeout"
                elif in_rollover_grace:
                    reason = "Stderr Progress Timeout (Segment Rollover Grace 초과)"
                else:
                    reason = "Stderr Progress Timeout"
                asyncio.create_task(self._trigger_auto_restart(reason))
                break

            # 3. 녹화 파일 크기 증가 감시 (rec+stream)
            if self.state == "rec+stream":
                # [수정] os.path.getsize를 채널 전용 스레드풀로 오프로드
                curr_size = await self._run_io(os.path.getsize, latest_file) if latest_file else 0

                # ------ 추가할 부분: 실제 파일 크기로 텔레메트리 데이터 업데이트 ------
                self.file_size = curr_size
                # --------------------------------------------------------------------

                if curr_size > 0 and curr_size == self.last_file_size:
                    # [수정] 폴링 간격이 이제 랜덤(0.5~1초)이라 "횟수 x 0.5초"로는 실제 경과 시간을
                    # 정확히 못 재므로, 크기가 안 바뀌기 시작한 실제 시각을 기준으로 경과시간을 잰다.
                    if self.file_stagnant_since_ts is None:
                        self.file_stagnant_since_ts = now
                    effective_stagnation_timeout = self.cfg.file_stagnation_timeout_sec * grace_multiplier
                    if now - self.file_stagnant_since_ts >= effective_stagnation_timeout:
                        # 변경: await -> asyncio.create_task
                        reason = "File Size Stagnation (Segment Rollover Grace 초과)" if in_rollover_grace else "File Size Stagnation"
                        asyncio.create_task(self._trigger_auto_restart(reason))
                        break
                else:
                    self.file_stagnant_since_ts = None
                    self.last_file_size = curr_size

                # [추가] rec_path 남은 용량 감시. 다 차면 ffmpeg가 그 자리에서 write 에러로 죽어
                # 녹화 파일이 깨질 수 있으므로, 여유 있게(MIN_REC_PATH_FREE_GB) 미리 감지해 STOP
                # 버튼을 누른 것과 동일하게 정지시킨다. disk_usage 자체가 매 tick(0.5~1초)마다
                # 돌기엔 과하므로 DISK_CHECK_INTERVAL_SEC마다 한 번만 확인한다.
                if now - self.last_disk_space_check_ts >= DISK_CHECK_INTERVAL_SEC:
                    self.last_disk_space_check_ts = now
                    try:
                        usage = await self._run_io(shutil.disk_usage, self.cfg.rec_path)
                        free_gb = usage.free / (1024 ** 3)
                    except Exception as e:
                        # 확인 자체가 실패한 경우는 오탐 방지를 위해 정지시키지 않고 에러만 남긴다.
                        logger.error(f"[{self.cfg.name}] rec_path 남은 용량 확인 실패 ({self.cfg.rec_path}): {e}")
                        free_gb = None
                    if free_gb is not None and free_gb < MIN_REC_PATH_FREE_GB:
                        asyncio.create_task(self._trigger_disk_low_stop(free_gb))
                        break

            # ffreport 파일 크기 갱신 ([수정] 채널 전용 스레드풀로 오프로드)
            if await self._run_io(os.path.exists, self.ffreport_path):
                self.fflog_size = await self._run_io(os.path.getsize, self.ffreport_path)

    async def _trigger_auto_restart(self, reason: str):
        # 이미 재시작이 진행 중이라면 중복 실행 방지
        if self.user_stopped or getattr(self, "is_restarting", False):
            return
        
        self.is_restarting = True
        target_state = self.state
        # [수정] "왜" 정지 후 재시작하는지(reason)를 명확히 남기고, [AUTO-RESTART] 태그를 붙여
        # server info 로그 패널(index.html)에서 다른 로그보다 눈에 띄게 강조 표시되도록 한다.
        logger.warning(
            f"[AUTO-RESTART][{self.cfg.name}] 사유: {reason} | 정지 시점 상태: {target_state} | "
            f"{self.cfg.auto_restart_hold_time}초 대기 후 재시작합니다."
        )
        # ffmpeg 비정상 종료/이상 감지 원인 파악을 위해 fflog tail 10줄 로깅
        self._log_fflog_tail(f"자동 재시작 트리거 ({reason})")

        # [수정] start()/stop()과 동일한 락으로 직렬화 -> 자동재시작이 진행되는 도중 사용자가
        # STOP/REC을 눌러도 self.process를 놓고 경합하지 않는다.
        try:
            async with self._lifecycle_lock:
                await self._stop_process()
        except Exception as e:
            logger.error(f"[{self.cfg.name}] 프로세스 종료 중 에러: {e}")
            
        await asyncio.sleep(self.cfg.auto_restart_hold_time)
        
        # [핵심 방어 코드] 3초 대기하는 동안 사용자가 STOP을 눌렀다면 재시작을 취소!
        # =========================================================
        if self.user_stopped:
            logger.info(f"[{self.cfg.name}] 대기 중 사용자 중지 명령 수신. 자동 재기동을 취소합니다.")
            self.is_restarting = False
            return
        # =========================================================
        
        logger.warning(f"[AUTO-RESTART][{self.cfg.name}] 자동 재기동 시작 (복구 타깃 상태: {target_state})")
        
        # TRANSFER 체크 상태 및 녹화 파일명 태그, 스케줄 소유권도 그대로 유지한 채로 재시작
        # (세그먼트 롤오버 등으로 인한 auto-restart는 "새로운 세션"이 아니라 같은 녹화의 연속이므로,
        # 이 녹화가 스케줄러가 시작한 것이었다면 재시작 후에도 계속 그 스케줄의 소유로 남아야 한다)
        await self.start(target_state,
            getattr(self, "transfer_enabled", False),
            getattr(self, "test_enabled", False),
            getattr(self, "rec_tag", ""),
            schedule_id=getattr(self, "started_by_schedule", None))

    async def _trigger_disk_low_stop(self, free_gb: float):
        """[추가] rec_path 남은 용량이 MIN_REC_PATH_FREE_GB 미만으로 떨어지면, STOP 버튼을 누른 것과
        완전히 동일하게(enc.stop() + manual_recording_active 해제) 정지시킨다. 자동 재시작(auto-restart)
        이 아니라 명시적 정지인 이유: 디스크가 그대로면 재시작해봐야 곧바로 다시 같은 문제가 반복될
        뿐이라, 재시도로 해결될 문제가 아니기 때문이다. manual_recording_active도 함께 꺼서, 디스크
        공간을 확보하지 않은 채 서버가 재시작돼도 같은 채널이 자동으로 다시 녹화를 시작하지 않게 한다
        (encoder_action()의 'stop' 처리와 동일한 마무리)."""
        logger.error(
            f"[{self.cfg.name}] rec_path 남은 용량 부족으로 자동 정지 "
            f"(남은 용량={free_gb:.1f}GB, 기준={MIN_REC_PATH_FREE_GB}GB, 경로={self.cfg.rec_path})"
        )
        await self.stop()
        await save_encoder_fields_to_config(self.cfg.id, {"manual_recording_active": False})

    def _log_fflog_tail(self, reason: str):
        """STOP 또는 ffmpeg 비정상 종료 시 원인 파악을 위해 fflog(ffreport) 파일의 마지막 10줄을 로깅한다."""
        try:
            if not self.ffreport_path or not os.path.exists(self.ffreport_path):
                logger.warning(f"[{self.cfg.name}] {reason} - fflog 파일을 찾을 수 없습니다: {self.ffreport_path}")
                return
            with open(self.ffreport_path, "r", encoding="utf-8", errors="ignore") as f:
                tail_lines = deque(f, maxlen=10)
            if not tail_lines:
                logger.warning(f"[{self.cfg.name}] {reason} - fflog 파일이 비어 있습니다: {self.ffreport_path}")
                return
            tail_text = "".join(tail_lines).rstrip("\n")
            logger.info(f"[{self.cfg.name}] {reason} - fflog tail (최근 10줄, {self.ffreport_path}):\n{tail_text}")
        except Exception as e:
            logger.error(f"[{self.cfg.name}] fflog tail 읽기 실패: {e}")

    async def get_info(self) -> dict:
        transfer_copied = sum(stat["copied"] for stat in self.transfer_stats.values())
        transfer_total = sum(stat["total"] for stat in self.transfer_stats.values())

        # 현재 녹화 중인 최신 파일 경로 가져오기
        latest_file = await self._get_latest_rec_file()
        current_file_name = os.path.basename(latest_file) if (self.state == "rec+stream" and latest_file) else ""


        return {
            "id": self.cfg.id,
            "name": self.cfg.name,
            "state": self.state,
            "progress_time": self.progress_time,
            "speed": self.speed,
            "file_size": self.file_size,
            "fflog_size": self.fflog_size,
            "transfer_enabled": getattr(self, "transfer_enabled", False),
            "test_enabled": getattr(self, "test_enabled", False),  # <--- 새로 추가
            "transfer_active": len(self.transfer_stats) > 0,
            "transfer_copied": transfer_copied,
            "transfer_total": transfer_total,
            "current_file": current_file_name,  # 추가: 순수 파일명만 전달
            "rec_tag": getattr(self, "rec_tag", ""),  # 마지막으로 사용/저장된 파일명 태그 (페이지 로딩/새로고침 시 텍스트 박스 복원용)
            # [추가] 지금 rec+stream이 사용자의 REC 버튼(수동)으로 시작된 건지, 예약 스케줄러가
            # 시작시킨 건지 화면에서 구분해서 보여주기 위함. None이면 수동, 아니면 그 스케줄의 id.
            "started_by_schedule": getattr(self, "started_by_schedule", None),
        }


    async def _get_latest_rec_file(self) -> Optional[str]:
        """rec_path 안에서 이 인코더 이름 패턴과 일치하는 파일 중 가장 최근(ctime) 것을 찾는다.
        [수정] os.listdir/os.path.getctime을 직접 부르면 디스크가 바쁠 때 이벤트루프를 블로킹할
        수 있어, 검색 로직 전체를 스레드로 오프로드한다."""
        def _do():
            try:
                if not os.path.exists(self.cfg.rec_path):
                    return None
                # 텍스트 박스 입력에서 온 태그가 파일명 맨 앞(헤더)에 "_"로 구분되어 붙을 수 있으므로 이를 허용
                pattern = re.compile(rf"^(?:.+_)?{re.escape(self.cfg.name)}_\d{{8}}_\d{{6}}\.mxf$")
                valid = [os.path.join(self.cfg.rec_path, f) for f in os.listdir(self.cfg.rec_path) if pattern.match(f)]
                return max(valid, key=os.path.getctime) if valid else None
            except Exception:
                return None
        return await self._run_io(_do)

    async def _get_latest_rec_size(self) -> int:
        latest = await self._get_latest_rec_file()
        try:
            return await self._run_io(os.path.getsize, latest) if latest else 0
        except Exception:
            return 0

    async def _transfer_manager_loop(self):
        # [수정] file_target_root 가 NFS 등 네트워크 경로일 때 os.makedirs 가 이벤트루프를
        # 블로킹하지 않도록 채널 전용 스레드풀에서 실행 (ffmpeg 파이프 읽기/SSE 브로드캐스트 정지 방지)
        await self._run_io(lambda: os.makedirs(self.cfg.file_target_root, exist_ok=True))
        
        while self.state == "rec+stream" and self.transfer_enabled:
            # [추가] 에러가 발생해 Abort 스위치가 켜지면 매니저 루프 즉시 탈출
            if getattr(self, "transfer_aborted", False):
                break
            try:
                # 폴더 전체를 뒤지지 않고, 워치독이 사용하는 최신 파일 탐색 함수만 호출
                latest_file = await self._get_latest_rec_file()
                
                # 최신 파일이 존재하고, 아직 처리한 목록에 없는 '새로운 파일'일 때만 딱 한 번 실행
                if latest_file and latest_file not in self.transfer_handled_files:
                    self.transfer_handled_files.add(latest_file)

                    # [추가] 서버 재시작 직후에는, 새 ffmpeg가 아직 자기 세그먼트 파일을 만들기 전
                    # 짧은 순간 rec_path에 남아있는 "이전 세션의 마지막 파일"이 ctime 기준 최신으로
                    # 잡힐 수 있다. 이번 세션 시작 시각(session_start_ts)보다 먼저 생성된 파일이면
                    # 이번 세션과 무관한 leftover이므로 전송 대상에서 제외한다.
                    try:
                        # [수정] os.path.getctime을 채널 전용 스레드풀로 오프로드
                        latest_ctime = await self._run_io(os.path.getctime, latest_file)
                    except OSError:
                        latest_ctime = 0

                    if latest_ctime < self.session_start_ts:
                        logger.info(
                            f"[{self.cfg.name}] 이전 세션의 leftover 파일로 판단해 전송 제외: "
                            f"{os.path.basename(latest_file)} (ctime={latest_ctime:.1f} < "
                            f"session_start_ts={self.session_start_ts:.1f})"
                        )
                    else:
                        dst_path = os.path.join(self.cfg.file_target_root, os.path.basename(latest_file))

                        # 워커(Worker)를 단 한 번만 생성
                        task = asyncio.create_task(self._transfer_worker(latest_file, dst_path))
                        self.transfer_workers.add(task)
                        task.add_done_callback(self.transfer_workers.discard)
                    
            except Exception as e:
                logger.error(f"[{self.cfg.name}] Transfer Manager Error: {e}")
                
            # 워치독과 동일하게 0.5초 주기로 가볍게 1개의 파일만 확인
            await asyncio.sleep(0.5)

    async def _verify_transfer_destination_writable(self, target_root: str) -> bool:
        """[추가] 실제 MXF 전송을 시작하기 전에, file_target_root가 존재하고 실제로 쓰기 가능한지
        랜덤 파일명의 작은 테스트 파일을 직접 써봐서 확인한다. NFS 마운트가 끊겼거나, 권한이 없거나,
        경로 자체가 사라진 경우를 미리 걸러내 무의미한 전송 시도(및 open() 실패로 인한 지연/에러)를 막는다.
        os.access()만으로는 NFS stale mount, 쿼터 초과, 원격 권한 불일치 같은 경우를 못 잡아낼 수 있어
        실제 write+delete를 직접 수행해 검증한다."""
        def _do_check():
            if not os.path.isdir(target_root):
                raise NotADirectoryError(f"경로가 존재하지 않거나 디렉터리가 아님: {target_root}")
            test_path = os.path.join(target_root, f".svcr_writetest_{uuid.uuid4().hex}.tmp")
            with open(test_path, "wb") as f:
                f.write(os.urandom(64))
                f.flush()
                os.fsync(f.fileno())
            os.remove(test_path)

        try:
            # target_root가 NFS 등 네트워크 경로일 수 있으므로 이벤트루프를 블로킹하지 않도록 채널 전용 스레드풀에서 실행
            await self._run_io(_do_check)
            return True
        except Exception as e:
            logger.error(f"[{self.cfg.name}] Transfer 대상 경로 쓰기 검증 실패 ({target_root}): {e}")
            return False

    async def _verify_transfer_destination_has_space(self, target_root: str) -> bool:
        """[추가] 전송 시작 전, file_target_root 남은 용량이 MIN_TARGET_FREE_GB 미만이면 전송을
        시작하지 않는다. 이 채널의 녹화 자체(rec_path)와는 무관하므로, 용량 부족이어도 녹화는
        계속되고 이번 파일의 전송만 건너뛴다(다음 파일이 생성될 때 다시 검사된다)."""
        try:
            # disk_usage도 NFS 경로에서 블로킹될 수 있어 채널 전용 스레드풀에서 실행
            usage = await self._run_io(shutil.disk_usage, target_root)
            free_gb = usage.free / (1024 ** 3)
        except Exception as e:
            logger.error(f"[{self.cfg.name}] Transfer 대상 경로 용량 확인 실패 ({target_root}): {e}")
            return False
        if free_gb < MIN_TARGET_FREE_GB:
            logger.error(
                f"[{self.cfg.name}] Transfer 시작 안 함 (대상 경로 남은 용량 부족: "
                f"{free_gb:.1f}GB < {MIN_TARGET_FREE_GB}GB): {target_root}"
            )
            return False
        return True

    async def _transfer_worker(self, src_file: str, dst_file: str):
        BUFFER_SIZE = 20_000_000         # 20MB
        HEADER_SIZE = 100_000            # 100KB
        FOOTER_OFFSET = 100_000_000      # 100MB

        # [추가] 5채널이 거의 동시에 세그먼트를 새로 만들면 전송 시작(파일 열기, 쓰기 검증 등)도
        # 한꺼번에 몰려서 디스크 I/O 큐가 순간적으로 쌓일 수 있다. 인코더마다 2~6초 사이에서
        # 랜덤하게 흩어서 전송 시작 시점을 어긋나게 한다.
        await asyncio.sleep(random.uniform(2.0, 6.0))

        for _ in range(20):
            if await self._run_io(os.path.exists, src_file): break
            await asyncio.sleep(0.5)
        else: return

        # [추가] 전송을 무조건 시도하기 전에, 대상 경로가 실제로 쓰기 가능한 상태인지 먼저 검증한다.
        # 검증에 실패하면 이 파일의 전송은 건너뛴다 (녹화 자체는 로컬 rec_path에 영향받지 않고 계속됨).
        if not await self._verify_transfer_destination_writable(self.cfg.file_target_root):
            logger.error(f"[{self.cfg.name}] Transfer 건너뜀 (대상 경로 검증 실패): {dst_file}")
            return

        # [추가] 남은 용량도 함께 확인한다 (로그는 헬퍼 안에서 남김).
        if not await self._verify_transfer_destination_has_space(self.cfg.file_target_root):
            return

        f_src = None
        f_dst = None
        self.transfer_stats[src_file] = {"copied": 0, "total": 0}

        # [핵심 개선] 스레드 내부에서 반복문을 돌도록 동기(Sync) 래퍼 함수 생성
        # 스레드 생성 오버헤드를 99% 이상 줄이고 I/O 효율을 극대화합니다.
        def sync_copy_chunk(read_limit: int):
            bytes_left = read_limit
            while bytes_left > 0:
                # [추가] 강제 종료(Ctrl-C)이거나 에러로 인한 Abort 발생 시 스레드 탈출
                if getattr(self, "is_shutting_down", False) or getattr(self, "transfer_aborted", False):
                    break
                chunk = f_src.read(min(BUFFER_SIZE, bytes_left))
                if not chunk: break
                f_dst.write(chunk)
                bytes_left -= len(chunk)
            return f_dst.tell()
        logger.info(f"[{self.cfg.name}] Transfer Started: {dst_file}")

        try:
            # [수정] dst_file(file_target_root)이 NFS 경로인 경우 open()이 네트워크 RPC로
            # 블로킹될 수 있어, 이벤트루프를 멈추지 않도록 채널 전용 스레드풀에서 실행
            f_src = await self._run_io(open, src_file, 'rb')
            f_dst = await self._run_io(open, dst_file, 'wb')
            size_src_old = -1

            # --- Phase 1: Incremental Copy ---
            while True:
                # [강제 종료 탈출구 2] 0.5초 대기 무한 루프 도중 탈출
                if getattr(self, "is_shutting_down", False):
                    break
                size_src = await self._run_io(os.path.getsize, src_file)
                size_dst = f_dst.tell()

                self.transfer_stats[src_file]["total"] = size_src

                if size_src > size_dst:
                    to_read = size_src - size_dst

                    # 0.5초 동안 쌓인 데이터를 백그라운드 스레드 하나에서 한 번에 복사
                    new_dst_pos = await self._run_io(sync_copy_chunk, to_read)
                    self.transfer_stats[src_file]["copied"] = new_dst_pos

                latest = await self._get_latest_rec_file()
                is_active = (self.state == "rec+stream") and (src_file == latest)

                if size_src == size_src_old:
                    if not is_active:
                        # [추가] growing이 멈춘 시점을 명확히 로그로 남긴다.
                        logger.info(
                            f"[{self.cfg.name}] File Growing 멈춤 감지: {os.path.basename(src_file)} "
                            f"(size={size_src}). {self.cfg.footer_update_delay_sec}초 대기 후 footer 갱신 확인."
                        )
                        await asyncio.sleep(self.cfg.footer_update_delay_sec)
                        if await self._run_io(os.path.getsize, src_file) > f_dst.tell():
                            logger.info(
                                f"[{self.cfg.name}] 대기 중 파일이 추가로 증가함: {os.path.basename(src_file)} "
                                f"-> footer 갱신을 보류하고 복사를 재개합니다."
                            )
                            continue
                        break
                size_src_old = size_src
                await asyncio.sleep(0.5)  # 0.5초 대기 후 다시 검사 (CPU 휴식)

            # =========================================================
            # [추가된 로직] 헤더/푸터 업데이트 전 최종 파일 용량 검증
            # =========================================================
            final_src_size = await self._run_io(os.path.getsize, src_file)
            final_dst_size = f_dst.tell()

            if final_src_size != final_dst_size:
                logger.warning(f"[{self.cfg.name}] 최종 파일 용량 불일치 감지 (src: {final_src_size} != dst: {final_dst_size}). 누락 데이터 동기화 시도.")

                # 원본이 더 크다면 누락된 만큼 마지막으로 한 번 더 복사
                if final_src_size > final_dst_size:
                    to_read = final_src_size - final_dst_size
                    await self._run_io(sync_copy_chunk, to_read)

                # 재검증 후에도 용량이 다르면 파일이 깨질 위험이 있으므로 즉시 작업 중단
                resync_check = await self._run_io(os.path.getsize, src_file)
                if resync_check != f_dst.tell():
                    raise RuntimeError(f"용량 동기화 실패 (src: {resync_check} != dst: {f_dst.tell()}). 헤더/푸터 업데이트를 중단합니다.")
            # =========================================================

            # --- Phase 2: Header Update ---
            if await self._run_io(os.path.getsize, src_file) > 0:
                f_src.seek(0)
                f_dst.seek(0)
                await self._run_io(sync_copy_chunk, HEADER_SIZE)

            # --- Phase 3: Footer Update ---
            total_size = await self._run_io(os.path.getsize, src_file)
            offset = min(FOOTER_OFFSET, total_size)
            if offset > 0:
                # [추가] footer 갱신이 실제로 시작되는 시점을 명확히 로그로 남긴다.
                logger.info(
                    f"[{self.cfg.name}] Footer 갱신 시작: {os.path.basename(dst_file)} "
                    f"(offset={offset} bytes, total_size={total_size} bytes)"
                )
                f_src.seek(-offset, 2)
                f_dst.seek(-offset, 2)
                await self._run_io(sync_copy_chunk, offset)

# Abort 되지 않고 정상적으로 끝났을 때만 로그 출력
            if not getattr(self, "transfer_aborted", False):
                logger.info(f"[{self.cfg.name}] Transfer Finished: {dst_file}")
        except Exception as e:
            logger.error(f"[{self.cfg.name}] Transfer failed {src_file}: {e}")
        finally:
            # [수정] NFS 상의 f_dst.close()는 flush/commit RPC로 블로킹될 수 있어 채널 전용 스레드풀로 오프로드
            if f_src: await self._run_io(f_src.close)
            if f_dst: await self._run_io(f_dst.close)
            self.transfer_stats.pop(src_file, None)

    # =====================================================================
    # [추가] bmxtranswrap 기반 새 transfer 방식 (기존 _transfer_manager_loop/_transfer_worker는
    # 위에 그대로 남겨두고, start()에서의 호출만 이쪽으로 바꿔서 대체한다).
    # =====================================================================

    async def _bmx_transfer_manager_loop(self):
        """새 세그먼트 파일이 생길 때마다 bmxtranswrap 전송을 하나씩 새로 띄운다.
        구조는 기존 _transfer_manager_loop와 동일 - 최신 rec 파일만 가볍게 폴링하고,
        이번 세션 시작 이전에 만들어진 leftover 파일은 건너뛴다."""
        await self._run_io(lambda: os.makedirs(self.cfg.file_target_root, exist_ok=True))

        while self.state == "rec+stream" and self.transfer_enabled:
            if getattr(self, "transfer_aborted", False):
                break
            try:
                latest_file = await self._get_latest_rec_file()

                if latest_file and latest_file not in self.bmx_transfer_handled_files:
                    self.bmx_transfer_handled_files.add(latest_file)

                    try:
                        latest_ctime = await self._run_io(os.path.getctime, latest_file)
                    except OSError:
                        latest_ctime = 0

                    if latest_ctime < self.session_start_ts:
                        logger.info(
                            f"[{self.cfg.name}] 이전 세션의 leftover 파일로 판단해 bmxtranswrap 전송 제외: "
                            f"{os.path.basename(latest_file)} (ctime={latest_ctime:.1f} < "
                            f"session_start_ts={self.session_start_ts:.1f})"
                        )
                    else:
                        dst_path = os.path.join(self.cfg.file_target_root, os.path.basename(latest_file))
                        task = asyncio.create_task(self._bmx_launch_transfer(latest_file, dst_path))
                        self.bmx_transfer_launch_tasks.add(task)
                        task.add_done_callback(self.bmx_transfer_launch_tasks.discard)

            except Exception as e:
                logger.error(f"[{self.cfg.name}] bmxtranswrap Transfer Manager Error: {e}")

            await asyncio.sleep(0.5)

    async def _bmx_launch_transfer(self, src_file: str, dst_file: str):
        """실제 bmxtranswrap 프로세스를 띄우기 전, 스태거 대기 + 대상 경로 쓰기 가능 여부 확인을
        (async 쪽에서) 수행한다. 이 두 단계를 통과해야만 별도 스레드를 실제로 만든다."""
        # [추가] REC 시작 후 5초 + 2~6초 랜덤. 여러 채널이 거의 동시에 세그먼트를 새로 만들 때
        # bmxtranswrap 실행이 한꺼번에 몰리지 않도록 흩어준다.
        await asyncio.sleep(5.0 + random.uniform(2.0, 6.0))

        if not await self._verify_transfer_destination_writable(self.cfg.file_target_root):
            logger.error(f"[{self.cfg.name}] bmxtranswrap 전송 건너뜀 (대상 경로 쓰기 검증 실패): {dst_file}")
            return

        # [추가] 남은 용량도 함께 확인한다 (로그는 헬퍼 안에서 남김).
        if not await self._verify_transfer_destination_has_space(self.cfg.file_target_root):
            return

        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._bmx_transfer_thread_func,
            args=(src_file, dst_file, stop_event),
            name=f"bmx-transfer-{self.cfg.name}",
            daemon=True,
        )
        self.bmx_transfer_stop_events[src_file] = stop_event
        self.bmx_transfer_threads[src_file] = thread
        thread.start()

    def _bmx_transfer_thread_func(self, src_file: str, dst_file: str, stop_event: threading.Event):
        """별도 스레드에서 동작하는 동기 함수. bmxtranswrap subprocess 하나의 시작부터 끝까지
        (진행 중 www UI용 크기 갱신, graceful shutdown 신호 감시 포함)를 전담한다.
        - growing 도중 읽기 실패에 대한 재시도는 --gf/--gf-retries/--gf-delay로 bmxtranswrap
          자신에게 맡긴다. main.py는 프로세스가 오류로 끝나도 절대 재시작하지 않는다.
        - --rt 1 로 실시간 속도로 처리하므로, 소스가 계속 growing하는 동안은 bmxtranswrap도
          그 페이스를 따라가다가, growing이 멈추면(재시도가 --gf-retries만큼 실패하면) 스스로
          정상 종료한다 - 우리 쪽에서 "그만 성장한다"는 신호를 따로 줄 필요가 없다.
        """
        self.transfer_stats[src_file] = {"copied": 0, "total": 0}
        cmd = [
            "bmxtranswrap",
            "-t", "rdd9",
            "--single-pass",
            "--part", "300",
            "--gf-rate", "1",
            "--rt", "1",
            "--gf",
            "--gf-retries", str(DEFAULT_RETRY),
            "--gf-delay", str(RETRY_DELAY),
            "-o", dst_file,
            src_file,
        ]
        # [수정] 파일마다 별도 로그 파일을 만들지 않는다. bmxtranswrap 자체의 진행 상황(stdout/
        # stderr) 출력은 버리고(DEVNULL), 시작/종료(코드)만 메인 로그(logger)에 남긴다. DEVNULL로
        # 버려도 파이프가 꽉 차서 bmxtranswrap의 write()가 막히는 일은 없다 (커널이 계속 흡수).
        logger.info(f"[{self.cfg.name}] bmxtranswrap Transfer Started: {dst_file} (cmd: {' '.join(cmd)})")

        returncode = None
        graceful_shutdown = False  # 우리가 의도적으로 SIGTERM을 보내서 끝난 것인지 구분 (진짜 오류와 다르게 로그)

        # [추가] dst_file은 NFS(file_target_root) 위에 있어, os.path.getsize(dst_file)가 hard-mount
        # 장애로 D-state에 멈출 수 있다. 이 진행률 조회를 메인 제어 루프(아래 while True) 안에서 직접
        # 하면, 멈춘 동안 stop_event 체크/proc.wait()까지 못 돌아와서 종료 신호에 응답을 못 하게 된다.
        # 그래서 진행률 갱신은 별도의 감시 스레드로 완전히 분리한다 - 이 스레드가 NFS 때문에 영영
        # 멈추더라도(daemon=True) 메인 제어 루프와 프로세스 종료/reap 로직에는 영향을 주지 않는다.
        watch_done = threading.Event()

        def _watch_progress():
            while not watch_done.is_set():
                try:
                    src_size = os.path.getsize(src_file)
                    dst_size = os.path.getsize(dst_file) if os.path.exists(dst_file) else 0
                    # 이미 전송이 끝나 정리된 뒤라면(watch_done 세팅 후) transfer_stats를 되살리지 않는다 -
                    # getsize가 멈춰있다가 뒤늦게 돌아온 경우를 대비한 안전장치.
                    if not watch_done.is_set():
                        self.transfer_stats[src_file] = {"copied": dst_size, "total": src_size}
                except OSError:
                    pass
                watch_done.wait(0.5)

        watch_thread = threading.Thread(
            target=_watch_progress, name=f"bmx-progress-{self.cfg.name}", daemon=True
        )
        watch_thread.start()

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                while True:
                    try:
                        returncode = proc.wait(timeout=0.5)
                        break
                    except subprocess.TimeoutExpired:
                        if stop_event.is_set():
                            graceful_shutdown = True
                            logger.info(f"[{self.cfg.name}] bmxtranswrap 종료 신호 수신, "
                                        f"PID {proc.pid}로 SIGTERM 전송: {dst_file}")
                            proc.terminate()
                            try:
                                returncode = proc.wait(timeout=self.cfg.graceful_stop_timeout_sec)
                            except subprocess.TimeoutExpired:
                                logger.warning(f"[{self.cfg.name}] bmxtranswrap graceful 종료 시간 "
                                               f"초과, SIGKILL 전송: {dst_file}")
                                proc.kill()
                                returncode = proc.wait()
                            break
            except Exception:
                # wait() 도중 예외가 나도 프로세스가 좀비로 남지 않도록 최소한 한 번은 정리 시도
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                raise
        except Exception as e:
            logger.error(f"[{self.cfg.name}] bmxtranswrap 실행 중 예외 발생: {e}")
            returncode = returncode if returncode is not None else -1
        finally:
            # [추가] 감시 스레드 종료 신호. 그 스레드가 지금 hung getsize()에 멈춰있다면 이 시점엔
            # 못 빠져나가지만(daemon=True라 프로세스 종료를 막지는 않음), 정상적인 경우엔 다음
            # 폴링 주기(최대 0.5초) 안에 스스로 끝난다.
            watch_done.set()
            self.bmx_transfer_stop_events.pop(src_file, None)
            self.bmx_transfer_threads.pop(src_file, None)
            self.transfer_stats.pop(src_file, None)

        # [추가] 오류 종료(returncode != 0)여도 main.py는 재시작하지 않는다 - 재시도는 전적으로
        # bmxtranswrap의 --gf-retries/--gf-delay 몫이다. 여기서는 결과(종료 코드)만 로그로 남긴다.
        # graceful shutdown(우리가 의도적으로 SIGTERM/SIGKILL을 보낸 경우)은 "오류"가 아니라
        # "중단됨"으로 구분해서 로그를 남긴다 - returncode가 0이 아니어도 정상적인 상황이기 때문.
        if returncode == 0:
            logger.info(f"[{self.cfg.name}] bmxtranswrap Transfer Finished (returncode=0): {dst_file}")
        elif graceful_shutdown:
            logger.info(f"[{self.cfg.name}] bmxtranswrap Transfer 중단됨 (종료 요청, returncode={returncode}): "
                        f"{dst_file}")
        else:
            logger.error(f"[{self.cfg.name}] bmxtranswrap Transfer failed (returncode={returncode}): {dst_file}")


# =====================================================================
# 예약 녹화 스케줄러
# (과거 AutoHotkey로 만들었던 scheduler2.ahk를 대체: 정해진 시각에 특정 인코더에
#  녹화 시작/종료 명령을 보내는 구조. 날짜를 지정하면 1회성, 요일을 지정하면 매주 반복.)
# =====================================================================

SCHEDULE_SAVE_LOCK = asyncio.Lock()


def _parse_hhmm(s: str):
    h, m = s.split(":")
    h, m = int(h), int(m)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"올바르지 않은 시:분 값: {s}")
    return h, m


def _duration_minutes(start_time: str, end_time: str) -> int:
    """start_time 대비 end_time까지의 분(minute) 길이를 반환한다. end_time이 start_time보다
    같거나 이르면 자정을 넘겨 다음날로 이어지는 것으로 간주(기존 _concrete_range와 동일한 규칙).
    이 규칙 때문에 표현 가능한 최대 길이는 항상 24시간 미만(최대 23:59)이 되는데, 그걸 명시적으로
    검증해서 "예약 녹화 최대 길이는 24시간 미만"이라는 제약을 코드로 못박아 둔다."""
    sh, sm = _parse_hhmm(start_time)
    eh, em = _parse_hhmm(end_time)
    start_minutes = sh * 60 + sm
    end_minutes = eh * 60 + em
    if end_minutes <= start_minutes:
        end_minutes += 24 * 60
    return end_minutes - start_minutes


@dataclass
class ScheduleEntry:
    id: str
    encoder_id: int
    title: str = ""                       # 녹화 파일명 태그로 쓰일 예약 타이틀 (rec_tag)
    transfer_enabled: bool = False
    start_time: str = "00:00"             # "HH:MM"
    end_time: str = "00:00"               # "HH:MM" (start_time보다 같거나 이르면 자정을 넘기는 것으로 간주)
    date: str = ""                        # "YYYY-MM-DD". repeat_days가 비어있을 때(1회성)만 사용
    repeat_days: List[int] = field(default_factory=list)  # 0=월 ... 6=일. 비어있으면 1회성
    enabled: bool = True
    created_at: str = ""
    # [추가] "정지 전용" 예약: start_time 없이 end_time만 실행한다. 그 시각이 되면 이 인코더가
    # rec+stream 상태이기만 하면 그게 수동으로 시작한 녹화든 다른 스케줄이 시작한 녹화든 상관없이
    # 무조건 정지시킨다 (사용자가 "이 채널은 이 시각에 무조건 끝내고 싶다"고 명시적으로 등록한 것이므로
    # 소유권 검사를 생략). 겹침 검증에도 참여하지 않는다 (예약된 시간대를 점유하는 게 아니라, 그 순간
    # 강제 종료만 하는 것이므로 다른 예약과 "겹친다"는 개념 자체가 성립하지 않음).
    stop_only: bool = False


class SchedulerManager:
    def __init__(self, path: str):
        self.path = path
        self.entries: Dict[str, ScheduleEntry] = {}
        # [추가] 매 tick(5초)마다 "(스케줄 id, 기준일) 조합이 이미 시작/종료 처리됐는지"를 기억해,
        # 같은 회차를 중복으로 여러 번 트리거하지 않게 한다. 서버 재시작 시 초기화되는 건 의도된
        # 동작 — 재시작 자체가 이미 모든 인코더를 free로 되돌리기 때문에 다시 판단해도 안전하다.
        # [수정] 예전엔 entry.id 하나만 키로 쓰는 Dict[str,str]이었는데, 반복 요일에 연속된 이틀
        # (예: 월,화 모두 포함)이 들어있으면 같은 tick 안에서 "어제" 앵커와 "오늘" 앵커가 서로
        # 다른 key(날짜)로 이 딕셔너리를 번갈아 덮어써서, 두 앵커가 서로의 dedup 기록을 계속
        # 지워버리는 바람에 종료/시작 로그가 5초마다 무한 반복되는 버그가 있었다(실사고로 발견됨).
        # (스케줄 id, 기준일 key) 조합 자체를 집합의 원소로 취급하면 이 문제가 사라진다.
        self._last_start_key: Set[Tuple[str, str]] = set()
        self._last_stop_key: Set[Tuple[str, str]] = set()

    def load(self):
        self.entries = {}
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for item in data.get("schedules", []):
                entry = ScheduleEntry(
                    id=item["id"],
                    encoder_id=item["encoder_id"],
                    title=item.get("title", ""),
                    transfer_enabled=item.get("transfer_enabled", False),
                    start_time=item.get("start_time", "00:00"),
                    end_time=item.get("end_time", "00:00"),
                    date=item.get("date", ""),
                    repeat_days=list(item.get("repeat_days", [])),
                    enabled=item.get("enabled", True),
                    created_at=item.get("created_at", ""),
                    # [수정] 이 필드가 빠져있어서, 서버가 재시작될 때마다 저장돼있던 "정지 전용"
                    # 예약이 전부 일반 예약(stop_only=False, dataclass 기본값)으로 되돌아가는 버그가
                    # 있었다. 정지 전용 예약은 start_time == end_time으로 저장되는데, 일반 예약으로
                    # 취급되면 _concrete_range()가 "자정을 넘긴다"고 판단해 그 시각부터 거의 24시간
                    # 가까운 구간을 "예약 창"으로 오인하고, 그 구간 안 아무 때나 재시작하면 실제로
                    # REC가 저절로 시작돼버렸다 (실사고 재현/확인됨).
                    stop_only=item.get("stop_only", False),
                )
                self.entries[entry.id] = entry
        except Exception as e:
            logger.error(f"scheduler.json 로드 실패: {e}")
            self.entries = {}

    async def save(self):
        def _do():
            data = {"schedules": [asdict(e) for e in self.entries.values()]}
            tmp_path = self.path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.path)
        async with SCHEDULE_SAVE_LOCK:
            await asyncio.to_thread(_do)

    # ---------------- 겹침 검증 ----------------

    def _occurs_on(self, entry: ScheduleEntry, d: date) -> bool:
        if entry.repeat_days:
            return d.weekday() in entry.repeat_days
        return entry.date == d.isoformat()

    def _concrete_range(self, entry: ScheduleEntry, anchor: date):
        sh, sm = _parse_hhmm(entry.start_time)
        eh, em = _parse_hhmm(entry.end_time)
        start_dt = datetime(anchor.year, anchor.month, anchor.day, sh, sm)
        end_dt = datetime(anchor.year, anchor.month, anchor.day, eh, em)
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        return start_dt, end_dt

    def find_conflict(self, encoder_id: int, start_time: str, end_time: str, sched_date: str,
                       repeat_days: List[int], exclude_id: Optional[str] = None) -> Optional[ScheduleEntry]:
        """같은 인코더에 대해 시간대가 실제로 겹칠 수 있는 기존 스케줄을 찾아 반환한다 (없으면 None).
        요일 반복 스케줄은 특정 날짜가 없으므로, "요일이 겹칠 수 있는가"를 -1/0/+1일 오프셋으로
        따져서(자정을 넘기는 녹화가 다음날 새벽 스케줄과 겹치는 경우까지 포함) 판단한다."""
        candidate = ScheduleEntry(id="__candidate__", encoder_id=encoder_id, start_time=start_time,
                                   end_time=end_time, date=sched_date, repeat_days=list(repeat_days))
        for other in self.entries.values():
            # [추가] 정지 전용 예약은 시간대를 점유하는 게 아니라 그 순간 강제 종료만 하는 것이므로
            # 겹침 검증 대상에서 제외한다.
            if other.id == exclude_id or other.encoder_id != encoder_id or not other.enabled or other.stop_only:
                continue
            for offset in (-1, 0, 1):
                # candidate가 기준일(anchor)에 발생한다고 가정했을 때, other가 (anchor+offset)에도
                # 발생할 수 있는지로 "두 스케줄이 같은 주/같은 시점 근처에 겹칠 수 있는가"를 판단한다.
                if candidate.repeat_days:
                    anchor_weekdays = candidate.repeat_days
                else:
                    try:
                        anchor_weekdays = [date.fromisoformat(candidate.date).weekday()]
                    except Exception:
                        continue
                hit = False
                for wd in anchor_weekdays:
                    other_wd = (wd + offset) % 7
                    if other.repeat_days:
                        if other_wd in other.repeat_days:
                            hit = True
                            break
                    else:
                        try:
                            if date.fromisoformat(other.date).weekday() == other_wd:
                                hit = True
                                break
                        except Exception:
                            continue
                if not hit:
                    continue
                anchor = date(2024, 1, 1 + wd)  # 임의의 기준 월요일(2024-01-01은 월요일)로부터 wd만큼 이동
                a_start, a_end = self._concrete_range(candidate, anchor)
                b_start, b_end = self._concrete_range(other, anchor + timedelta(days=offset))
                if a_start < b_end and b_start < a_end:
                    return other
        return None

    # ---------------- 조회/직렬화 ----------------

    def compute_status(self, entry: ScheduleEntry, now: datetime) -> str:
        if not entry.enabled:
            return "disabled"

        if entry.stop_only:
            # [추가] 정지 전용 예약은 "그 순간까지"라는 구간 개념이 없어 active가 될 수 없다.
            # 반복 예약은 매번 다음 회차를 기다리는 것이므로 계속 upcoming, 1회성은 지나면 completed.
            if entry.repeat_days:
                return "upcoming"
            try:
                d = date.fromisoformat(entry.date)
            except Exception:
                return "upcoming"
            eh, em = _parse_hhmm(entry.end_time)
            end_dt = datetime(d.year, d.month, d.day, eh, em)
            return "completed" if now >= end_dt else "upcoming"

        for anchor in (now.date() - timedelta(days=1), now.date()):
            if not self._occurs_on(entry, anchor):
                continue
            start_dt, end_dt = self._concrete_range(entry, anchor)
            if start_dt <= now < end_dt:
                return "active"
        if entry.repeat_days:
            return "upcoming"
        try:
            d = date.fromisoformat(entry.date)
        except Exception:
            return "upcoming"
        start_dt, end_dt = self._concrete_range(entry, d)
        return "completed" if now >= end_dt else "upcoming"

    def entries_as_list(self) -> list:
        now = datetime.now()
        result = []
        for entry in sorted(self.entries.values(), key=lambda e: (e.date or "", e.start_time)):
            d = asdict(entry)
            d["status"] = self.compute_status(entry, now)
            result.append(d)
        return result

    # ---------------- 실행 (tick) ----------------

    def _try_start(self, enc: "FFmpegEncoderWrapper", entry: ScheduleEntry, key: str):
        if enc.state == "rec+stream":
            if enc.started_by_schedule is None:
                logger.info(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 시작 시각 도달했지만 "
                            f"수동 REC 진행 중이라 건너뜁니다.")
                self._last_start_key.add((entry.id, key))  # 이번 회차는 스킵 확정 (재시도 안 함)
            # else: 다른 스케줄이 아직 안전 종료 중 -> key를 기록하지 않고 다음 tick에 재확인 (대기)
            return
        if enc.state not in ("free", "stream"):
            return
        self._last_start_key.add((entry.id, key))
        asyncio.create_task(self._do_start(enc, entry))

    async def _do_start(self, enc: "FFmpegEncoderWrapper", entry: ScheduleEntry):
        try:
            await enc.start("rec+stream", transfer_enabled=entry.transfer_enabled, rec_tag=entry.title,
                             schedule_id=entry.id)
            await save_encoder_fields_to_config(enc.cfg.id, {"rec_tag": enc.rec_tag, "transfer_enabled": entry.transfer_enabled})
            logger.info(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 예약 녹화 시작")
        except Exception as e:
            logger.error(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 시작 실패: {e}")

    def _try_stop(self, enc: "FFmpegEncoderWrapper", entry: ScheduleEntry, key: str):
        self._last_stop_key.add((entry.id, key))
        if enc.state != "rec+stream":
            return  # 이미 멈춰있음 (수동 STOP 등) - 스케줄러가 할 일 없음
        # [추가] "정지 전용" 예약은 이 채널의 녹화가 누구 소유인지(수동/다른 스케줄) 따지지 않고
        # 무조건 정지한다 - 사용자가 "이 시각엔 무조건 끝낸다"고 명시적으로 등록한 것이기 때문.
        if not entry.stop_only and enc.started_by_schedule != entry.id:
            logger.info(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 종료 시각 도달했지만 "
                        f"이 스케줄이 시작한 녹화가 아니라 건너뜁니다.")
            return
        asyncio.create_task(self._do_stop(enc, entry))

    async def _do_stop(self, enc: "FFmpegEncoderWrapper", entry: ScheduleEntry):
        try:
            await enc.stop()
            logger.info(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 예약 녹화 종료")
        except Exception as e:
            logger.error(f"[스케줄러][{enc.cfg.name}] '{entry.title or entry.id}' 종료 실패: {e}")

    def forget_entry(self, entry_id: str):
        """이 스케줄 id와 관련된 dedup 기록(어떤 기준일이든)을 전부 지운다. 예약 삭제 시 호출."""
        self._last_start_key = {k for k in self._last_start_key if k[0] != entry_id}
        self._last_stop_key = {k for k in self._last_stop_key if k[0] != entry_id}

    async def tick(self, encoders: Dict[int, "FFmpegEncoderWrapper"]):
        now = datetime.now()

        # [추가] 안전장치: 시스템 시계가 일시적으로 틀어진 상태(예: 부팅 직후 NTP 동기화 전, 시계가
        # 과거의 특정 날짜를 가리키고 있던 순간)에 우연히 오래된 1회성 스케줄의 날짜와 일치해 실제로
        # 녹화가 시작될 수 있다. 이후 시계가 정상 시각으로 교정되면 그 스케줄은 더 이상 "오늘/어제"
        # 범위에 들지 않게 되어, 정상 종료 로직(아래 for문)이 그 스케줄을 아예 건드리지 않게 되고
        # 결과적으로 녹화가 영원히 방치된다 (실측 재현 확인됨). 매 tick마다, 스케줄이 시작시킨 녹화 중
        # 그 스케줄이 더 이상 유효 범위가 아닌 것을 찾아 강제로 정지시켜 이런 고아 녹화를 방지한다.
        for enc in encoders.values():
            if enc.state != "rec+stream" or not enc.started_by_schedule:
                continue
            owner = self.entries.get(enc.started_by_schedule)
            stale = (owner is None) or (not owner.enabled) or (
                not owner.stop_only
                and not self._occurs_on(owner, now.date())
                and not self._occurs_on(owner, now.date() - timedelta(days=1))
            )
            if stale:
                logger.warning(
                    f"[스케줄러][{enc.cfg.name}] 이 채널을 시작시킨 예약(id={enc.started_by_schedule})이 "
                    f"더 이상 유효한 시간대가 아닙니다 (시계 오차 등으로 과거 스케줄이 잘못 실행됐을 "
                    f"가능성). 고아 녹화 방지를 위해 강제로 정지합니다."
                )
                asyncio.create_task(enc.stop())

        for entry in list(self.entries.values()):
            if not entry.enabled:
                continue
            enc = encoders.get(entry.encoder_id)
            if not enc:
                continue

            if entry.stop_only:
                # [추가] 정지 전용 예약: start_time/자정넘김 개념이 없다. 오늘(또는 지정한 날짜)이
                # 발생일이면 end_time 시각에 무조건 정지만 시도한다.
                anchor = now.date()
                if not self._occurs_on(entry, anchor):
                    continue
                eh, em = _parse_hhmm(entry.end_time)
                end_dt = datetime(anchor.year, anchor.month, anchor.day, eh, em)
                key = anchor.isoformat()
                if now >= end_dt and (entry.id, key) not in self._last_stop_key:
                    self._try_stop(enc, entry, key)
                continue

            for anchor in (now.date() - timedelta(days=1), now.date()):
                if not self._occurs_on(entry, anchor):
                    continue
                start_dt, end_dt = self._concrete_range(entry, anchor)
                key = anchor.isoformat()
                if start_dt <= now < end_dt and (entry.id, key) not in self._last_start_key:
                    self._try_start(enc, entry, key)
                if now >= end_dt and (entry.id, key) not in self._last_stop_key:
                    self._try_stop(enc, entry, key)


scheduler = SchedulerManager("")  # lifespan에서 실제 경로로 재설정 후 load()


# -------------------- 변경된 부분 --------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup (시작될 때 실행) ---
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    if not os.path.isabs(config_path):
        config_path = os.path.join(BASE_DIR, config_path)

    with open(config_path, "r", encoding="utf-8") as f:
        conf_data = json.load(f)

    for item in conf_data.get("encoders", []):
        cfg = EncoderConfig(
            id=item["id"],
            name=item["name"],
            input_params=item.get("input_params", ""),  # <--- 새로 추가
            segment_time=item.get("segment_time", 60),
            rec_path=item.get("rec_path", f"./recordings/enc{item['id']}"),
            stream_target_root=item.get("stream_target_root", "rtsp://127.0.0.1:8554"),
            file_target_root=item.get("file_target_root", f"./recordings/enc{item['id']}"),
            stderr_timeout_sec=item.get("stderr_timeout_sec", 8.0),
            auto_restart_hold_time=item.get("auto_restart_hold_time", 3.0),
            graceful_stop_timeout_sec=item.get("graceful_stop_timeout_sec", 20.0),
            footer_update_delay_sec=item.get("footer_update_delay_sec", 20.0),
            file_stagnation_timeout_sec=item.get("file_stagnation_timeout_sec", 8.0),
            segment_rollover_grace_sec=item.get("segment_rollover_grace_sec", 15.0),
            ffmpeg_custom_params=item.get("ffmpeg_custom_params", ""),
            rec_tag=item.get("rec_tag", ""),
            prefer_nvenc=item.get("prefer_nvenc", None),  # 인코더별 오버라이드. 없으면 전역 기본값 사용
            transfer_enabled=item.get("transfer_enabled", False),
            manual_recording_active=item.get("manual_recording_active", False),
            test_enabled=item.get("test_enabled", False),
        )
        encoders[cfg.id] = FFmpegEncoderWrapper(cfg, broadcast_status)

    global CONFIG_PATH
    CONFIG_PATH = config_path

    # [추가] transfer 방식 전역 선택. 채널별 오버라이드는 지원하지 않는다. 아래 재개 루프가
    # enc.start()를 호출하기 전에 먼저 값을 정해둬야 한다(start()가 이 값을 보고 방식을 고른다).
    global BMXTRANSWRAP_ENABLED
    BMXTRANSWRAP_ENABLED = bool(conf_data.get("bmxtranswrap", False))
    logger.info(f"Transfer 방식: {'bmxtranswrap' if BMXTRANSWRAP_ENABLED else 'binary copy (기존 방식)'} "
                f"(config.json bmxtranswrap={BMXTRANSWRAP_ENABLED})")

    # [추가] main.py가 재시작되기 직전(수동 재시작/크래시 후 자동 재시작/재시작 버튼 등 무엇이든)
    # 수동으로 REC 중이었던 채널은, 서버가 다시 뜨면 자동으로 녹화를 재개시킨다. 예약 스케줄러가
    # 시작한 녹화는 이 로직과 무관하게 tick()이 알아서 재개하므로 건드리지 않는다.
    for enc in encoders.values():
        if enc.cfg.manual_recording_active:
            logger.warning(
                f"[{enc.cfg.name}] 재시작 전 수동 녹화 중이었던 채널을 자동으로 재개합니다 "
                f"(rec_tag='{enc.cfg.rec_tag}', transfer_enabled={enc.cfg.transfer_enabled}, "
                f"test_enabled={enc.cfg.test_enabled})."
            )
            asyncio.create_task(enc.start(
                "rec+stream",
                transfer_enabled=enc.cfg.transfer_enabled,
                test_enabled=enc.cfg.test_enabled,
                rec_tag=enc.cfg.rec_tag,
            ))

    # [추가] 예약 녹화 스케줄 로드 (scheduler.json이 없거나 비어 있으면 빈 목록으로 시작 -> 리스트뷰에 아무것도 안 뜸)
    scheduler.path = os.path.join(BASE_DIR, "scheduler.json")
    scheduler.load()

    # [추가] RTSP 미리보기 스트림 인코딩 설정 로드 + NVENC 실동작 여부 검사
    global STREAM_ENCODE_CFG, NVENC_AVAILABLE
    STREAM_ENCODE_CFG = conf_data.get("stream_video_encode", {})
    STREAM_ENCODE_CFG.setdefault("prefer_nvenc", DEFAULT_STREAM_ENCODE_CFG["prefer_nvenc"])
    STREAM_ENCODE_CFG.setdefault("nvenc_max_sessions", DEFAULT_STREAM_ENCODE_CFG["nvenc_max_sessions"])
    STREAM_ENCODE_CFG.setdefault("libx264", DEFAULT_STREAM_ENCODE_CFG["libx264"])
    STREAM_ENCODE_CFG.setdefault("h264_nvenc", DEFAULT_STREAM_ENCODE_CFG["h264_nvenc"])

    NVENC_AVAILABLE = await asyncio.to_thread(detect_nvenc_available)
    per_enc_nvenc = ", ".join(
        f"{wrapper.cfg.name}={'전역기본값' if wrapper.cfg.prefer_nvenc is None else wrapper.cfg.prefer_nvenc}"
        for wrapper in encoders.values()
    ) if encoders else "(인코더 없음)"
    logger.info(
        f"NVENC 사용 가능 여부: {NVENC_AVAILABLE} "
        f"(전역 prefer_nvenc={STREAM_ENCODE_CFG['prefer_nvenc']}, "
        f"nvenc_max_sessions={STREAM_ENCODE_CFG['nvenc_max_sessions']}) "
        f"| 인코더별 설정: {per_enc_nvenc}"
    )

    # [수정] 무한 루프 태스크를 변수에 담아서 기억해 둡니다.
    status_task = asyncio.create_task(status_tick_loop())
    server_info_task = asyncio.create_task(server_info_tick_loop())
    no_client_task = asyncio.create_task(no_client_auto_stop_loop())
    scheduler_task = asyncio.create_task(scheduler_tick_loop())

    yield  # 앱 실행 구간

    # --- Shutdown (종료될 때 실행) ---
    status_task.cancel()  # 1. UI 갱신 무한 루프 즉시 파괴
    server_info_task.cancel()
    no_client_task.cancel()
    scheduler_task.cancel()
    # [수정] 순차 종료(for + await) 대신 동시 종료로 변경 -> 인코더 5개가 각자 최대 5초씩
    # 걸릴 수 있는 graceful ffmpeg 종료를 직렬로 기다리지 않고 병렬로 처리해 전체 종료 시간을 단축
    await asyncio.gather(
        *(enc.stop(force=True) for enc in encoders.values()),
        return_exceptions=True
    )  # 2. 모든 인코더 및 복사 스레드에 강제 종료 지시

    # [추가] 채널별 전용 IO 스레드풀도 프로세스 종료 시 정리 (남은 스레드가 프로세스 종료를 지연시키지 않도록)
    for enc in encoders.values():
        enc._io_executor.shutdown(wait=False, cancel_futures=True)

def get_current_df_timecode() -> str:
    """
    현재 시스템 로컬 시간 기준으로 29.97fps Drop-Frame 타임코드 문자열 생성
    형식: HH:MM:SS;FF (SMPTE 표준 규격)
    """
    now = datetime.now()
    frame = int((now.microsecond / 1_000_000.0) * 29.97)
    frame = min(frame, 29)
    
    # 드롭 프레임 규칙: 10분 단위를 제외한 매 분 시작(00초) 시 00, 01번 프레임을 스킵
    if now.second == 0 and (now.minute % 10 != 0) and frame < 2:
        frame = 2
        
    return f"{now.strftime('%H:%M:%S')};{frame:02d}"
    
    

# FastAPI 앱 객체를 lifespan과 함께 생성
app = FastAPI(lifespan=lifespan)
# -----------------------------------------------------
encoders: Dict[int, FFmpegEncoderWrapper] = {}
event_queues: List[asyncio.Queue] = []


async def broadcast_event(event_name: str, data):
    payload = json.dumps(data)
    # [수정] 이 for문 안에서 await로 매번 양보하는 동안, 다른 브라우저의 연결/접속해제가
    # event_queues.append()/remove()를 동시에 호출할 수 있다. 원본 리스트를 그대로 순회하면
    # 그 사이 목록이 바뀌어 일부 클라이언트가 이번 브로드캐스트를 못 받고 건너뛸 수 있으므로,
    # 순회 시작 시점의 스냅샷(사본)을 순회한다.
    for q in list(event_queues):
        await q.put((event_name, payload))


async def broadcast_status():
    # [수정] get_info()가 이제 비동기(디스크 조회를 스레드로 오프로드)이므로 gather로 동시에 수집
    data = await asyncio.gather(*(enc.get_info() for enc in encoders.values()))
    await broadcast_event("status", list(data))


async def broadcast_server_info():
    await broadcast_event("server_info", get_server_info())


async def status_tick_loop():
    while True:
        await asyncio.sleep(0.2)
        if event_queues:
            await broadcast_status()


async def server_info_tick_loop():
    while True:
        await asyncio.sleep(1.0)
        if event_queues:
            await broadcast_server_info()


# [추가] www UI에 접속 중인 브라우저(SSE 클라이언트)가 0명인 상태로 이 시간(초) 이상 지속되면
# STREAM 전용(미리보기만) 채널을 자동으로 정지한다. 아무도 안 보는데 굳이 미리보기용
# libx264 인코딩/스트리밍을 계속 돌릴 이유가 없기 때문. REC 중(rec+stream)인 채널은 실제
# 녹화가 목적이라 클라이언트 유무와 무관하게 절대 건드리지 않는다.
NO_CLIENT_AUTO_STOP_SEC = 10.0


async def no_client_auto_stop_loop():
    zero_client_since: Optional[float] = None
    already_triggered = False
    while True:
        await asyncio.sleep(1.0)
        if event_queues:
            # 클라이언트가 하나라도 있으면 카운트/트리거 상태를 리셋
            zero_client_since = None
            already_triggered = False
            continue

        now = time.time()
        if zero_client_since is None:
            zero_client_since = now
            already_triggered = False
            continue

        if not already_triggered and (now - zero_client_since) >= NO_CLIENT_AUTO_STOP_SEC:
            already_triggered = True  # 같은 무클라이언트 구간 안에서 반복 트리거 방지
            for enc in encoders.values():
                if enc.state == "stream":
                    logger.warning(
                        f"[{enc.cfg.name}] www 접속 클라이언트 0명 상태가 {NO_CLIENT_AUTO_STOP_SEC:.0f}초 이상 "
                        f"지속되어 STREAM 채널을 자동 정지합니다 (REC 중인 채널은 영향 없음)."
                    )
                    asyncio.create_task(enc.stop())


async def broadcast_schedules():
    await broadcast_event("schedules", scheduler.entries_as_list())


async def scheduler_tick_loop():
    # [추가] 예약 녹화 실행 루프. status/server_info와 달리 접속자가 없어도(event_queues가 비어도)
    # 예약은 그대로 실행돼야 하므로 tick() 자체는 항상 수행하고, SSE 브로드캐스트만 접속자가 있을 때로 제한한다.
    while True:
        await asyncio.sleep(5.0)
        try:
            await scheduler.tick(encoders)
        except Exception as e:
            logger.error(f"스케줄러 tick 처리 중 오류: {e}")
        if event_queues:
            await broadcast_schedules()


@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open(os.path.join(BASE_DIR, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    # BASE_DIR 기준으로 ./favicon/favicon.ico 경로 생성
    favicon_path = os.path.join(BASE_DIR, "favicon", "favicon.ico")
    return FileResponse(favicon_path)

@app.get("/reader.js")
async def get_reader_js():
    return FileResponse(os.path.join(BASE_DIR, "reader.js"), media_type="application/javascript")


@app.get("/events")
async def sse_endpoint(request: Request):
    q = asyncio.Queue()
    event_queues.append(q)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                event_name, data = await q.get()
                yield {"event": event_name, "data": data}
        finally:
            event_queues.remove(q)

    return EventSourceResponse(event_generator())


@app.post("/api/encoder/{enc_id}/action")
async def encoder_action(enc_id: int, request: Request):
    payload = await request.json()
    action = payload.get("action")
    transfer = payload.get("transfer", False)  # 체크박스 값 수신
    test_enabled = payload.get("test", False)  # <--- 새로 추가
    rec_tag = payload.get("rec_tag", "")  # <--- 새로 추가: 녹화 파일명 태그 텍스트 박스 값
    # [추가] STREAM/REC/STOP을 어느 클라이언트가 눌렀는지 로그로 남기기 위한 접속 IP.
    # request.client가 None일 수 있는 극히 드문 경우(테스트 클라이언트 등)를 대비해 방어적으로 처리.
    client_ip = request.client.host if request.client else "unknown"

    enc = encoders.get(enc_id)
    if not enc:
        logger.warning(f"[API] 인코더 ID {enc_id}를 찾을 수 없습니다. (요청 action: {action}, client: {client_ip})")
        return {"status": "error", "message": "Encoder not found"}

    logger.info(f"[{enc.cfg.name}] API Action 수신 -> Action: '{action}', Transfer: {transfer}, Test: {test_enabled}, RecTag: '{rec_tag}', Client: {client_ip}")

    if action == "stream":
        # [수정] STREAM은 파일을 만들지 않으니 transfer_enabled를 실제로 쓰지는 않지만(전송 대상
        # 파일 자체가 없음), 화면의 TRANSFER 체크박스는 "다음 REC 때 쓸 선호값"으로 계속 보여야 한다.
        # 예전엔 free 상태에서 사용자가 체크박스만 바꾸고 STREAM을 누르면, 그 값이 요청에 아예 안
        # 실려서(REC일 때만 보냈었음) 서버에 남아있던 예전 값으로 화면이 도로 덮어써지는 문제가 있었다.
        # 이제 프론트엔드가 STREAM에도 현재 체크박스 값을 실어 보내므로 그대로 반영하고, config.json에도
        # 저장해 다음 REC/서버 재시작 때도 그대로 복원되게 한다.
        await enc.start("stream", transfer_enabled=transfer, test_enabled=test_enabled)
        # [수정] STREAM으로 전환하면 더 이상 "수동 녹화 중"이 아니므로 재개 플래그도 함께 끈다.
        # [수정] TEST 체크박스 상태도 transfer_enabled와 동일하게 저장한다 - 안 그러면 TEST 모드로
        # REC 중이던 채널이 예기치 않게 재시작됐을 때, 자동 재개 로직이 TEST 여부를 몰라 진짜
        # DeckLink 입력으로 복원돼버리는 사고가 난다(실사고로 발견됨).
        await save_encoder_fields_to_config(enc_id, {"transfer_enabled": transfer, "test_enabled": test_enabled, "manual_recording_active": False})
    elif action == "rec":
        await enc.start("rec+stream", transfer_enabled=transfer, test_enabled=test_enabled, rec_tag=rec_tag)
        # [수정] REC 클릭 시점의 파일명 태그 + TRANSFER/TEST 체크박스 상태를 함께 config.json에 저장해
        # 다음 서버 기동 시에도 그대로 복원되게 한다. manual_recording_active=True를 같이 저장해두면,
        # main.py가 재시작돼도(수동 재시작/크래시 후 자동 재시작/재시작 버튼) 기동 시 이 채널의
        # 녹화를 자동으로 재개할 수 있다.
        await save_encoder_fields_to_config(enc_id, {"rec_tag": enc.rec_tag, "transfer_enabled": transfer, "test_enabled": test_enabled, "manual_recording_active": True})
    elif action == "stop":
        await enc.stop()
        # [추가] 사용자가 명시적으로 STOP을 눌렀다는 건 더 이상 재개할 필요가 없다는 뜻이므로 플래그를 끈다.
        # (서버 종료 시 내부적으로 호출되는 강제 stop은 이 API를 거치지 않으므로 여기서 꺼지지 않고
        # 그대로 유지된다 - 그래야 재시작 후 자동 재개가 가능하다.)
        await save_encoder_fields_to_config(enc_id, {"manual_recording_active": False})

    await broadcast_status()
    return {"status": "ok", "state": enc.state}


def _validate_schedule_payload(payload: dict):
    encoder_id = payload.get("encoder_id")
    if encoder_id not in encoders:
        raise HTTPException(status_code=400, detail=f"존재하지 않는 인코더 id: {encoder_id}")

    stop_only = bool(payload.get("stop_only", False))
    end_time = payload.get("end_time", "")
    try:
        _parse_hhmm(end_time)
    except Exception:
        raise HTTPException(status_code=400, detail="end_time은 'HH:MM' 형식이어야 합니다")

    if stop_only:
        # [추가] 정지 전용 예약은 start_time을 쓰지 않는다 (dataclass 필드 채우기용으로 end_time과
        # 동일하게 둠 - tick()에서 stop_only는 이 값을 아예 참조하지 않으므로 실제로 쓰이진 않는다).
        start_time = end_time
    else:
        start_time = payload.get("start_time", "")
        try:
            _parse_hhmm(start_time)
        except Exception:
            raise HTTPException(status_code=400, detail="start_time은 'HH:MM' 형식이어야 합니다")
        if start_time == end_time:
            raise HTTPException(status_code=400, detail="시작 시각과 종료 시각이 같을 수 없습니다")
        # [추가] 예약 녹화 최대 길이는 24시간 미만(최대 23:59)으로 제한한다. start_time/end_time만으로
        # 자정을 넘기는 녹화를 표현하는 구조상 원래도 24시간 이상은 표현할 수 없지만, 그 제약을
        # 암묵적인 부작용이 아니라 명시적으로 검증해 못박아 둔다.
        duration = _duration_minutes(start_time, end_time)
        if duration >= 24 * 60:
            raise HTTPException(status_code=400, detail="예약 녹화 최대 길이는 24시간 미만(최대 23:59)이어야 합니다")

    repeat_days = payload.get("repeat_days") or []
    sched_date = payload.get("date", "") or ""
    if not repeat_days:
        try:
            date.fromisoformat(sched_date)
        except Exception:
            raise HTTPException(status_code=400, detail="반복 요일이 없으면 date('YYYY-MM-DD')를 지정해야 합니다")
    else:
        if any((not isinstance(d, int)) or d < 0 or d > 6 for d in repeat_days):
            raise HTTPException(status_code=400, detail="repeat_days는 0(월)~6(일) 사이의 정수 목록이어야 합니다")

    return encoder_id, start_time, end_time, sched_date, repeat_days, stop_only


@app.get("/api/schedules")
async def list_schedules():
    return scheduler.entries_as_list()


@app.post("/api/schedules")
async def create_schedule(request: Request):
    payload = await request.json()
    encoder_id, start_time, end_time, sched_date, repeat_days, stop_only = _validate_schedule_payload(payload)

    if not stop_only:
        conflict = scheduler.find_conflict(encoder_id, start_time, end_time, sched_date, repeat_days)
        if conflict:
            raise HTTPException(status_code=409, detail=f"같은 인코더의 기존 예약 '{conflict.title or conflict.id}'과(와) 시간이 겹칩니다")

    entry = ScheduleEntry(
        id=uuid.uuid4().hex,
        encoder_id=encoder_id,
        title=str(payload.get("title", ""))[:100],
        transfer_enabled=bool(payload.get("transfer_enabled", False)),
        start_time=start_time,
        end_time=end_time,
        date=sched_date,
        repeat_days=repeat_days,
        enabled=True,
        created_at=datetime.now().isoformat(timespec="seconds"),
        stop_only=stop_only,
    )
    scheduler.entries[entry.id] = entry
    await scheduler.save()
    await broadcast_schedules()
    logger.info(f"[스케줄러] 새 예약 생성: {entry.id} (encoder_id={encoder_id}, title='{entry.title}', stop_only={stop_only})")
    return {"status": "ok", "id": entry.id}


@app.put("/api/schedules/{sched_id}")
async def update_schedule(sched_id: str, request: Request):
    if sched_id not in scheduler.entries:
        raise HTTPException(status_code=404, detail="존재하지 않는 예약입니다")
    payload = await request.json()

    # enabled 토글만 하는 요청(다른 필드 없이 enabled만 옴)은 겹침 검증 없이 바로 반영
    if set(payload.keys()) <= {"enabled"}:
        scheduler.entries[sched_id].enabled = bool(payload.get("enabled", True))
        await scheduler.save()
        await broadcast_schedules()
        return {"status": "ok"}

    encoder_id, start_time, end_time, sched_date, repeat_days, stop_only = _validate_schedule_payload(payload)
    if not stop_only:
        conflict = scheduler.find_conflict(encoder_id, start_time, end_time, sched_date, repeat_days, exclude_id=sched_id)
        if conflict:
            raise HTTPException(status_code=409, detail=f"같은 인코더의 기존 예약 '{conflict.title or conflict.id}'과(와) 시간이 겹칩니다")

    entry = scheduler.entries[sched_id]
    entry.encoder_id = encoder_id
    entry.title = str(payload.get("title", ""))[:100]
    entry.transfer_enabled = bool(payload.get("transfer_enabled", False))
    entry.start_time = start_time
    entry.end_time = end_time
    entry.date = sched_date
    entry.repeat_days = repeat_days
    entry.enabled = bool(payload.get("enabled", entry.enabled))
    entry.stop_only = stop_only
    await scheduler.save()
    await broadcast_schedules()
    return {"status": "ok"}


@app.post("/api/schedules/{sched_id}/extend")
async def extend_schedule(sched_id: str):
    """[추가] 예약 목록 각 행의 "+10분" 버튼. 누를 때마다 종료 시각을 10분 뒤로 늦춘다.
    정지 전용(stop_only) 예약은 start_time==end_time 불변식(_validate_schedule_payload 참고)을
    유지해야 하므로 두 필드를 함께 옮기고, "총 녹화 길이" 개념이 없어 상한 없이 그대로 10분씩 이동한다.
    일반 예약은 총 길이가 24시간 미만(최대 23:59)을 넘지 않도록 증가분을 클램프하고, 그 결과 다른
    예약과 시간이 겹치게 되면(같은 인코더) 적용하지 않는다 - 겹침 금지는 생성/수정 때와 동일한 규칙이다."""
    if sched_id not in scheduler.entries:
        raise HTTPException(status_code=404, detail="존재하지 않는 예약입니다")
    entry = scheduler.entries[sched_id]

    if entry.stop_only:
        eh, em = _parse_hhmm(entry.end_time)
        new_total = (eh * 60 + em + 10) % (24 * 60)
        new_end = f"{new_total // 60:02d}:{new_total % 60:02d}"
        entry.end_time = new_end
        entry.start_time = new_end
    else:
        current_duration = _duration_minutes(entry.start_time, entry.end_time)
        new_duration = min(current_duration + 10, 24 * 60 - 1)
        sh, sm = _parse_hhmm(entry.start_time)
        new_total = (sh * 60 + sm + new_duration) % (24 * 60)
        new_end = f"{new_total // 60:02d}:{new_total % 60:02d}"

        conflict = scheduler.find_conflict(
            entry.encoder_id, entry.start_time, new_end, entry.date, entry.repeat_days, exclude_id=sched_id
        )
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=f"같은 인코더의 기존 예약 '{conflict.title or conflict.id}'과(와) 시간이 겹쳐 연장할 수 없습니다",
            )
        entry.end_time = new_end

    await scheduler.save()
    await broadcast_schedules()
    logger.info(f"[스케줄러] 예약 {sched_id} 종료 시각을 +10분 연장: {entry.end_time}")
    return {"status": "ok", "end_time": entry.end_time}


@app.delete("/api/schedules/{sched_id}")
async def delete_schedule(sched_id: str):
    if sched_id in scheduler.entries:
        del scheduler.entries[sched_id]
        scheduler.forget_entry(sched_id)
        await scheduler.save()
        await broadcast_schedules()
    return {"status": "ok"}


@app.post("/api/schedules/delete_completed")
async def delete_completed_schedules():
    """[추가] "완료된 예약 모두 삭제" 버튼. compute_status()가 "completed"로 판단하는 항목(1회성
    예약 중 이미 끝난 것)만 골라서 한꺼번에 지운다. 반복 요일 예약은 계속 반복되는 것이라
    "completed" 상태가 될 수 없으므로 이 버튼의 영향을 받지 않는다."""
    now = datetime.now()
    to_delete = [eid for eid, entry in scheduler.entries.items()
                 if scheduler.compute_status(entry, now) == "completed"]
    for eid in to_delete:
        del scheduler.entries[eid]
        scheduler.forget_entry(eid)
    if to_delete:
        await scheduler.save()
        await broadcast_schedules()
        logger.info(f"[스케줄러] 완료된 예약 {len(to_delete)}개 일괄 삭제: {to_delete}")
    return {"status": "ok", "deleted_count": len(to_delete)}


@app.get("/debug", response_class=HTMLResponse)
async def get_debug_page():
    with open(os.path.join(BASE_DIR, "debug.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.post("/debug/restart_main")
async def restart_main_process():
    """[추가] /debug 페이지의 "main.py 재시작" 버튼. 좀비처럼 남아있는 ffmpeg 프로세스까지
    호스트 전체에서 정리하고 main.py를 graceful하게 재시작한다.
    - 1) 이 앱이 추적 중인지와 무관하게, 호스트에 떠 있는 ffmpeg라는 이름의 프로세스 전부에
      SIGTERM을 보낸다 (과거 비정상 종료로 남은 고아 프로세스까지 정리하기 위함).
    - 2) 프로세스 자신에게 SIGTERM을 보내 uvicorn의 정상 종료 절차(lifespan shutdown - 인코더
      graceful stop, IO 스레드풀 정리 등 이미 다듬어둔 경로)를 그대로 태운다.
    - 3) systemd 유닛이 Restart=always/RestartSec=3 로 설정돼 있어(3개 호스트 전부 동일 확인됨),
      프로세스가 종료되면 몇 초 뒤 자동으로 다시 시작된다. 그래서 systemctl을 직접 호출할
      필요가 없고(원격 호스트에서 sudo 암호가 필요해 실패했던 문제도 자연히 피해간다), 이 방식은
      main.py가 어느 계정으로 돌든(root/sendust) 동일하게 동작한다."""
    logger.warning("[SYSTEM] /debug 페이지에서 main.py 재시작 요청 수신")

    killed_pids = []
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            if proc.info["name"] == "ffmpeg":
                proc.send_signal(signal.SIGTERM)
                killed_pids.append(proc.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    logger.warning(f"[SYSTEM] 호스트 내 ffmpeg 프로세스 {len(killed_pids)}개에 SIGTERM 전송: {killed_pids}")

    async def _self_terminate():
        # 응답이 먼저 브라우저로 나간 뒤에 종료 신호를 보내기 위해 살짝 지연시킨다.
        await asyncio.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.create_task(_self_terminate())
    return {"status": "ok", "killed_ffmpeg_pids": killed_pids}


@app.get("/debug/events")
async def debug_sse_endpoint(request: Request):
    async def event_generator():
        try:
            while True:
                # 클라이언트 연결이 끊어지면 루프 종료
                if await request.is_disconnected():
                    break
                
                # 시스템 자원 사용량 측정
                cpu_usage = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory()
                host_ip = request.client.host if request.client else "localhost"
                
                debug_data = {
                    "system": {
                        "cpu_percent": cpu_usage,
                        "memory_percent": mem.percent,
                        "memory_used_gb": round(mem.used / (1024**3), 2),
                        "memory_total_gb": round(mem.total / (1024**3), 2)
                    },
                    "encoders": []
                }

                for enc in encoders.values():
                    n = enc.cfg.id
                    
                    # 디스크 용량 계산
                    disk_free_gb = 0
                    disk_total_gb = 0
                    if os.path.exists(enc.cfg.rec_path):
                        usage = shutil.disk_usage(enc.cfg.rec_path)
                        disk_free_gb = round(usage.free / (1024**3), 2)
                        disk_total_gb = round(usage.total / (1024**3), 2)

                    # MediaMTX 경로 정보
                    paths = {
                        "Publisher (RTSP)": f"{enc.cfg.stream_target_root}/input{n}00",
                        "Viewer (WHEP Video)": f"http://{host_ip}:8889/input{n}00/whep",
                        "Viewer (WHEP Audio12)": f"http://{host_ip}:8889/input{n}12/whep"
                    }

                    debug_data["encoders"].append({
                        "id": n,
                        "name": enc.cfg.name,
                        "state": enc.state,
                        "rec_path": enc.cfg.rec_path,
                        "disk_free_gb": disk_free_gb,
                        "disk_total_gb": disk_total_gb,
                        "paths": paths,
                        "logs": list(enc.ffmpeg_logs)
                    })

                # JSON 문자열로 변환하여 SSE 데이터로 전송
                yield {"event": "debug_status", "data": json.dumps(debug_data)}
                
                # 1초 대기 후 다음 데이터 전송
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            pass
            
    return EventSourceResponse(event_generator())
if __name__ == "__main__":
    # [수정] timeout_graceful_shutdown 기본값(None)은 열려있는 커넥션이 전부 닫힐 때까지 무한 대기한다.
    # 브라우저의 SSE(/events)는 자동 재연결 특성상 스스로 끊기지 않아 재시작 시 uvicorn이 영원히
    # 멈춰 있다가 systemd TimeoutStopSec(90초) 이후 SIGKILL로 강제 종료되는 문제가 있었음.
    # 값을 짧게 주어 uvicorn이 알아서 커넥션을 정리하고 lifespan shutdown(ffmpeg graceful stop)으로
    # 넘어가게 한다.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False, timeout_graceful_shutdown=5)
