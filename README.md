# svcr2026

Blackmagic DeckLink 기반 다채널 방송 녹화/스트리밍 시스템입니다. FastAPI + asyncio + ffmpeg로 만들어졌으며, 웹 UI에서 여러 채널을 동시에 모니터링하고 녹화/스트리밍을 제어할 수 있습니다.

> 이 저장소는 실제 운영 환경(호스트명, IP, 운영 데이터 등)을 마스킹하고, 재현에 필요한 핵심 코드와 예시 설정만 정리해서 공개한 버전입니다.

## 주요 기능

- **다채널 동시 녹화 + RTSP 프리뷰 스트리밍**: DeckLink Quad 카드 등에서 채널별로 녹화(XDCAM HD422 MXF)와 저지연 RTSP 프리뷰(MediaMTX 연동)를 동시에 수행
- **방송 장비 스타일 오디오 레벨미터**: dBFS 단위, 로그 스케일, 색상 밴드(-18dBFS/-6dBFS 기준) + 피크 홀드 인디케이터
- **예약 녹화 스케줄러**: 특정 시각 자동 시작/종료, 요일 반복, 겹침 검증, "정지 전용" 예약(수동 녹화도 강제 종료), 목록에서 종료 시각 +10분 연장
- **자동 복구(워치독)**: stderr progress timeout, 파일 크기 정체(stagnation), stdout 에러 버스트 감지 시 자동 재시작
- **녹화 파일 자동 전송**: 로컬(rec_path) → NAS/NFS(file_target_root)로 완성된 세그먼트를 자동 전송. 두 가지 방식 중 `config.json`의 전역 설정으로 선택
  - 기본: binary copy + header/footer 동기화 방식
  - 선택: [bmxtranswrap](https://github.com/bbc/bmx) 외부 바이너리를 이용한 growing-file 전송(실시간 페이싱, 자체 재시도)
- **디스크 용량 안전장치**: 녹화 중 rec_path 여유 공간이 부족하면 자동 정지(사유 로깅), 전송 전 대상 경로 용량이 부족하면 전송을 시작하지 않음
- **설정 실시간 재반영**: 녹화 시작 시점마다 `rec_path`/`file_target_root`/`segment_time`/`ffmpeg_custom_params`를 `config.json`에서 다시 읽어옴 (서버를 재시작하지 않고도 변경 가능)
- **재시작 내구성**: 서버가 재시작돼도(수동/자동 업데이트/크래시 등) 수동 녹화 중이던 채널(TEST 모드 여부 포함)을 자동으로 복구
- **웹 UI**: 채널별 상태/로그/전송 진행률 표시, 서버 리소스(CPU/RAM) 및 현재 transfer 방식 표시, 전용 디버그 콘솔(`/debug`) 제공

## 아키텍처

- **백엔드**: FastAPI + Python asyncio. 채널(인코더)마다 `FFmpegEncoderWrapper` 인스턴스가 ffmpeg subprocess 하나를 전담 관리
- **상태 전파**: Server-Sent Events(SSE)로 모든 접속 브라우저에 실시간 상태 push
- **프런트엔드**: 프레임워크 없이 순수 HTML/JS (`index.html`), WebRTC 프리뷰는 [MediaMTX](https://github.com/bluenviron/mediamtx)의 `reader.js`(WHEP 클라이언트) 사용
- **동시성 격리**: 채널마다 전용 스레드풀을 둬서, 한 채널의 느린 디스크/NFS I/O가 다른 채널에 영향을 주지 않도록 설계

## 요구 사항

- Linux (Ubuntu 기준으로 개발/운영)
- Python 3.10+
- [DeckLink Desktop Video](https://www.blackmagicdesign.com/kr/support) 드라이버 + DeckLink 입력 장치를 지원하는 ffmpeg 빌드
- [MediaMTX](https://github.com/bluenviron/mediamtx) (RTSP/WHEP 프리뷰 중계)
- (선택) [bmxtranswrap](https://github.com/bbc/bmx) — `config.json`의 `bmxtranswrap: true`를 쓸 경우에만 필요

## 설치

```bash
git clone git@github.com:sendust/svcr2026.git
cd svcr2026

python3 -m venv .
./bin/pip install -r requirements.txt
```

## 설정

`config.example.json`을 복사해 `config.json`을 만들고, 실제 장비 환경에 맞게 수정합니다.

```bash
cp config.example.json config.json
```

주요 항목:

| 필드 | 설명 |
|---|---|
| `bmxtranswrap` | 전역 transfer 방식 선택 (`false`=binary copy, `true`=bmxtranswrap). 채널별 개별 설정 불가 |
| `encoders[].input_params` | ffmpeg 입력 옵션 (DeckLink 장치 지정 등) |
| `encoders[].rec_path` | 녹화 파일이 저장될 로컬 경로 |
| `encoders[].file_target_root` | 녹화 완료 후 전송될 대상 경로(NAS/NFS 등) |
| `encoders[].segment_time` | 녹화 세그먼트 길이(초) |
| `encoders[].ffmpeg_custom_params` | 녹화 스트림에 적용할 비디오 코덱/옵션 |

예약 녹화 데이터는 `scheduler.json`에 저장됩니다. 비어 있는 상태로 시작하려면:

```bash
cp scheduler.example.json scheduler.json
```

## 실행

```bash
./bin/python main.py config.json
```

기본적으로 `0.0.0.0:8000`에서 서비스되며, 브라우저로 `http://<호스트>:8000`에 접속하면 메인 UI, `http://<호스트>:8000/debug`에서 디버그 콘솔을 볼 수 있습니다.

### systemd 서비스 예시

```ini
[Unit]
Description=svcr2026 Multi-channel Recorder
After=network.target

[Service]
WorkingDirectory=/path/to/svcr2026
ExecStart=/path/to/svcr2026/bin/python /path/to/svcr2026/main.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

## 디렉토리 구조

```
svcr2026/
├── main.py                  # 백엔드 (FastAPI 앱, 인코더/스케줄러/transfer 로직)
├── index.html                # 메인 웹 UI
├── debug.html                 # 디버그 콘솔 UI
├── reader.js                  # MediaMTX WHEP 클라이언트 (WebRTC 프리뷰)
├── disk_cleanup.py            # 디스크 정리 보조 스크립트
├── config.example.json        # 인코더/스트림 설정 예시
├── scheduler.example.json     # 예약 녹화 데이터 예시(빈 상태)
├── requirements.txt
└── favicon/
```

## 알려진 제약

- DeckLink 캡처 카드 및 전용 드라이버 환경을 전제로 개발되었습니다. 다른 캡처 장비를 쓰려면 `input_params`/필터 구성을 직접 맞춰야 합니다.
- MXF(XDCAM HD422) 포맷에 맞춰 ffmpeg 파라미터가 구성되어 있습니다. 다른 포맷이 필요하면 `ffmpeg_custom_params`를 조정하세요.
- `bmxtranswrap` 방식은 해당 바이너리가 시스템에 설치되어 있어야 동작합니다.

## 라이선스

이 저장소의 `reader.js`는 [MediaMTX](https://github.com/bluenviron/mediamtx) 프로젝트(MIT License)에서 가져온 WHEP 클라이언트입니다. 그 외 코드는 별도 라이선스 표기가 없는 한 프로젝트 작성자(sendust)에게 저작권이 있습니다.
