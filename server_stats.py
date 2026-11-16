"""서버 상태(SERVER INFO 카드) 집계용 헬퍼 모듈.

main.py에서 분리한 이유: 인코더/스케줄러 핵심 로직과 무관한 "시스템 모니터링" 관심사
(IP/NIC 트래픽, CPU 모델/코어, NVENC/nvidia-smi 감지, GPU 상태 polling)를 한 곳에 모아
main.py의 길이를 줄이고, 이 모듈만 따로 테스트/재사용할 수 있게 하기 위함.
상태를 갖는 함수(get_net_io_rates, gpu_stats_tick_loop/get_gpu_stats)는 이 모듈
스코프의 전역 변수에 상태를 들고 있다 - main.py는 그 상태를 직접 건드리지 않고
이 모듈이 제공하는 함수를 통해서만 접근한다.
"""
import socket
import subprocess
import time
import logging
from typing import Dict, List, Optional, Tuple

import psutil

logger = logging.getLogger("ServerStats")


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


def get_ipv4_by_interface() -> Dict[str, List[str]]:
    """인터페이스 이름 -> 그 인터페이스가 가진 IPv4 주소 목록. 네트워크 트래픽 패널에서
    "IPv4를 가진 NIC만" 걸러내는 데 쓴다 (IPv4가 없는 가상 인터페이스 등은 제외)."""
    result: Dict[str, List[str]] = {}
    try:
        for ifname, addrs in psutil.net_if_addrs().items():
            ipv4s = [a.address for a in addrs if a.family == socket.AF_INET]
            if ipv4s:
                result[ifname] = ipv4s
    except Exception:
        pass
    return result


def get_cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    import platform
    return platform.processor() or "Unknown"


def detect_nvenc_available() -> bool:
    """h264_nvenc 인코더가 실제로 동작하는지 짧은 더미 인코딩으로 확인한다 (NVIDIA GPU/드라이버 유무 판별).
    nvidia-smi 등 별도 툴 설치 여부와 무관하게, ffmpeg가 실제로 NVENC를 호출할 수 있는지를 직접 검증한다.
    테스트 해상도는 64x64가 아닌 256x256을 쓴다 - NVENC H.264 인코더는 최소 프레임 크기 제약이 있어
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
            stderr_tail = result.stderr.decode("utf-8", errors="ignore").strip().splitlines()[-3:]
            logger.warning(f"NVENC 감지 실패 (returncode={result.returncode}): {' | '.join(stderr_tail)}")
        return result.returncode == 0
    except Exception:
        return False


def detect_nvidia_smi_available() -> bool:
    """nvidia-smi 바이너리가 실제로 응답하는지 확인한다 (GPU 상태 패널 표시 여부 결정용).
    NVENC 가능 여부(ffmpeg 더미 인코딩)와는 별개 검사다 - 드라이버는 있지만 ffmpeg 빌드에
    nvenc가 안 들어있는 경우도, 반대로 nvidia-smi가 없어도 NVENC는 되는 극단적 환경도 이론상
    있을 수 있어 서로 섞어 쓰지 않는다."""
    try:
        result = subprocess.run(["nvidia-smi", "-L"], capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


_GPU_QUERY_FIELDS = [
    "index", "name", "utilization.gpu", "utilization.encoder", "utilization.decoder",
    "memory.used", "memory.total", "power.draw", "power.limit", "temperature.gpu",
]

_gpu_stats_cache: List[dict] = []


def _query_nvidia_smi_once() -> List[dict]:
    """nvidia-smi를 한 번 호출해 GPU별 상태를 파싱한다. subprocess 호출이라 블로킹이므로
    항상 asyncio.to_thread()를 통해서만 불러야 한다 (gpu_stats_tick_loop 참고)."""
    cmd = ["nvidia-smi", f"--query-gpu={','.join(_GPU_QUERY_FIELDS)}", "--format=csv,noheader,nounits"]
    result = subprocess.run(cmd, capture_output=True, timeout=5)
    if result.returncode != 0:
        raise RuntimeError(f"nvidia-smi exit={result.returncode}: {result.stderr.decode(errors='ignore').strip()}")

    gpus = []
    for line in result.stdout.decode(errors="ignore").strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != len(_GPU_QUERY_FIELDS):
            continue
        row = dict(zip(_GPU_QUERY_FIELDS, parts))

        def _num(key, cast=float):
            try:
                return cast(row[key])
            except (ValueError, KeyError):
                return None

        gpus.append({
            "index": _num("index", int),
            "name": row.get("name", "?"),
            "gpu_util": _num("utilization.gpu"),
            "encoder_util": _num("utilization.encoder"),
            "decoder_util": _num("utilization.decoder"),
            "mem_used_mb": _num("memory.used"),
            "mem_total_mb": _num("memory.total"),
            "power_draw_w": _num("power.draw"),
            "power_limit_w": _num("power.limit"),
            "temp_c": _num("temperature.gpu"),
        })
    return gpus


async def gpu_stats_tick_loop(interval_sec: float = 2.0):
    """nvidia-smi는 프로세스 기동 비용이 있어(수십ms) 매 server_info tick(1초)마다 직접 부르지
    않는다. 이 루프가 별도 주기로(기본 2초) 캐시를 갱신하고, get_server_info()는 그 캐시를
    동기적으로 읽기만 한다 - 이벤트 루프를 블로킹하지 않으면서도 매 요청마다 subprocess를
    새로 띄우지 않는다."""
    global _gpu_stats_cache
    import asyncio
    while True:
        try:
            _gpu_stats_cache = await asyncio.to_thread(_query_nvidia_smi_once)
        except Exception as e:
            logger.error(f"nvidia-smi 조회 실패: {e}")
        await asyncio.sleep(interval_sec)


def get_gpu_stats() -> List[dict]:
    return _gpu_stats_cache


# {iface_name: (timestamp, bytes_sent, bytes_recv)} - 직전 샘플. 속도(=델타/경과시간) 계산용.
_last_net_io_sample: Optional[Tuple[float, Dict[str, Tuple[int, int]]]] = None


def get_net_io_rates() -> List[dict]:
    """IPv4를 가진 모든 네트워크 인터페이스의 순간 송수신 속도(Mbps)를 계산한다.
    호출될 때마다 현재 누적 바이트 카운터를 직전 호출 시점과 비교해 델타를 내므로, 최초
    호출(직전 샘플 없음)은 항상 0으로 보고한다 - 그 다음 호출부터 실제 값이 나온다."""
    global _last_net_io_sample

    now = time.time()
    try:
        counters = psutil.net_io_counters(pernic=True)
    except Exception:
        return []

    ipv4_by_iface = get_ipv4_by_interface()
    current_sample = {name: (c.bytes_sent, c.bytes_recv) for name, c in counters.items()}

    results = []
    prev_ts, prev_sample = _last_net_io_sample if _last_net_io_sample else (None, {})
    elapsed = (now - prev_ts) if prev_ts else None

    for ifname, ipv4s in ipv4_by_iface.items():
        if ifname not in current_sample:
            continue
        if ifname == "lo":
            # 루프백은 실제 네트워크 링크가 아니라 모니터링 가치가 없어 목록에서 제외
            continue
        sent, recv = current_sample[ifname]
        tx_mbps = rx_mbps = 0.0
        if elapsed and elapsed > 0 and ifname in prev_sample:
            prev_sent, prev_recv = prev_sample[ifname]
            # 카운터가 리셋/오버플로된 경우(delta<0) 이번 틱은 0으로 처리 (다음 틱부터 정상 복귀)
            tx_mbps = max(0.0, (sent - prev_sent) * 8 / elapsed / 1_000_000)
            rx_mbps = max(0.0, (recv - prev_recv) * 8 / elapsed / 1_000_000)
        results.append({
            "name": ifname,
            "ipv4": ipv4s,
            "tx_mbps": round(tx_mbps, 2),
            "rx_mbps": round(rx_mbps, 2),
        })

    _last_net_io_sample = (now, current_sample)
    return results


def format_uptime(seconds: float) -> str:
    """초를 "N일 HH:MM:SS"(1일 미만이면 "HH:MM:SS") 형태로 포맷한다."""
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}일 {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_host_uptime_str() -> str:
    """호스트(OS) 부팅 후 경과 시간. psutil.boot_time()은 커널이 기록한 실제 부팅 시각이라
    main.py 자체의 재시작 여부와 무관하게 항상 OS 기준 가동 시간을 보여준다."""
    try:
        return format_uptime(time.time() - psutil.boot_time())
    except Exception:
        return "-"


def get_service_uptime_str() -> str:
    """svcr2026(main.py) 프로세스 자체가 떠 있은 시간. psutil.Process().create_time()은
    OS가 기록한 실제 프로세스 생성 시각이라, main.py 안에서 별도로 시작 시각을 기억해둘
    필요 없이 항상 정확하다 - 수동 재시작/자동 재시작/systemd Restart=always 어느 경우든
    이 프로세스가 새로 뜬 순간부터 다시 0에서 센다."""
    try:
        return format_uptime(time.time() - psutil.Process().create_time())
    except Exception:
        return "-"
