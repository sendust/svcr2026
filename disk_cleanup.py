#!/usr/bin/env python3
import os
import sys
import json
import time
import shutil
import logging
import argparse
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "disk_cleanup.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("DiskCleanup")


def load_rec_paths(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        conf = json.load(f)

    paths = []
    for item in conf.get("encoders", []):
        rec_path = item.get("rec_path")
        if rec_path and rec_path not in paths:
            paths.append(rec_path)
    return paths


def get_disk_usage_percent(path: str) -> float:
    usage = shutil.disk_usage(path)
    return usage.used / usage.total * 100


def list_files_oldest_first(path: str):
    files = []
    for root, _dirs, filenames in os.walk(path):
        for fn in filenames:
            fp = os.path.join(root, fn)
            try:
                mtime = os.path.getmtime(fp)
            except OSError:
                continue
            files.append((mtime, fp))
    files.sort(key=lambda x: x[0])
    return [fp for _, fp in files]


def cleanup_path(rec_path: str, threshold: float):
    if not os.path.isdir(rec_path):
        logger.warning(f"경로가 존재하지 않습니다: {rec_path}")
        return

    try:
        usage_percent = get_disk_usage_percent(rec_path)
    except Exception as e:
        logger.error(f"[{rec_path}] 디스크 사용량 확인 실패: {e}")
        return

    if usage_percent <= threshold:
        logger.info(f"[{rec_path}] 디스크 사용량 {usage_percent:.1f}% (임계값 {threshold:.1f}% 이하, 삭제 불필요)")
        return

    logger.warning(f"[{rec_path}] 디스크 사용량 {usage_percent:.1f}% > 임계값 {threshold:.1f}%. 오래된 파일부터 삭제를 시작합니다.")

    candidates = list_files_oldest_first(rec_path)
    deleted_count = 0
    freed_bytes = 0

    while usage_percent > threshold:
        if not candidates:
            logger.error(f"[{rec_path}] 삭제할 파일이 더 이상 없습니다. 현재 사용량 {usage_percent:.1f}% (임계값 {threshold:.1f}%)")
            break

        target = candidates.pop(0)

        try:
            size = os.path.getsize(target)
            mtime = os.path.getmtime(target)
        except OSError:
            size = 0
            mtime = None

        try:
            os.remove(target)
        except Exception as e:
            logger.error(f"[삭제 실패] {target}: {e}. 다음으로 오래된 파일을 시도합니다.")
            continue

        deleted_count += 1
        freed_bytes += size
        mtime_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S") if mtime else "unknown"
        logger.info(f"[삭제됨] {target} ({size / (1024 ** 2):.1f} MB, mtime={mtime_str})")

        try:
            usage_percent = get_disk_usage_percent(rec_path)
        except Exception as e:
            logger.error(f"[{rec_path}] 디스크 사용량 재확인 실패: {e}")
            break

    logger.info(
        f"[{rec_path}] 정리 완료. 삭제 {deleted_count}개 파일, 확보 용량 {freed_bytes / (1024 ** 3):.2f} GB, "
        f"최종 사용량 {usage_percent:.1f}%"
    )


def run_cycle(config_path: str, threshold: float):
    try:
        rec_paths = load_rec_paths(config_path)
    except Exception as e:
        logger.error(f"config.json 로드 실패 ({config_path}): {e}")
        return

    if not rec_paths:
        logger.warning(f"config.json에서 rec_path를 찾을 수 없습니다: {config_path}")
        return

    logger.info(f"디스크 검사 시작 (대상 경로 {len(rec_paths)}개, 임계값 {threshold:.1f}%)")
    for rec_path in rec_paths:
        cleanup_path(rec_path, threshold)
    logger.info("디스크 검사 완료.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="disk_cleanup.py",
        description=(
            "config.json에 정의된 모든 rec_path의 디스크 사용량을 30분 주기로 감시하며, "
            "임계값(%)을 초과하면 가장 오래된 파일부터 삭제해 사용량을 임계값 이하로 낮춥니다."
        ),
    )
    parser.add_argument(
        "threshold",
        type=float,
        help="디스크 사용량 임계값(%%). 이 값을 초과하면 오래된 파일부터 삭제합니다. 예: 50",
    )
    parser.add_argument(
        "--config",
        default=os.path.join(BASE_DIR, "config.json"),
        help="config.json 경로 (기본값: 스크립트와 동일 위치의 config.json)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1800,
        help="검사 주기(초). 기본값: 1800초(30분)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="주기적으로 반복하지 않고 1회만 검사/정리하고 종료합니다.",
    )
    return parser


def main():
    parser = build_arg_parser()

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if not (0 < args.threshold < 100):
        parser.error("threshold는 0보다 크고 100보다 작은 값이어야 합니다.")

    logger.info(
        f"디스크 정리 스크립트 시작. config={args.config}, threshold={args.threshold}%, "
        f"interval={args.interval}초, once={args.once}"
    )

    try:
        while True:
            run_cycle(args.config, args.threshold)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("사용자 중단(Ctrl+C)으로 스크립트를 종료합니다.")


if __name__ == "__main__":
    main()
