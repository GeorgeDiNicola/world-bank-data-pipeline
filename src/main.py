import time

from world_bank_pipeline.pipeline import main


def format_elapsed_time(elapsed_seconds: float) -> str:
    total_seconds = int(round(elapsed_seconds))
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes}m {seconds}s"

    if minutes:
        return f"{minutes}m {seconds}s"

    return f"{seconds}s"


def timed_main() -> None:
    time_started_at = time.perf_counter()

    try:
        main()
    finally:
        elapsed_time = format_elapsed_time(time.perf_counter() - time_started_at)
        print(f"Spark pipeline elapsed time: {elapsed_time}", flush=True)


if __name__ == "__main__":
    timed_main()
