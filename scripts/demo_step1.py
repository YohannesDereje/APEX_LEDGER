import json
import sys
import time


def main() -> None:
    with open("scripts/demo_data.json", "r", encoding="utf-8") as demo_file:
        events = json.load(demo_file)

    number_of_events = len(events)
    delay = 55 / number_of_events if number_of_events else 0

    start_time = time.time()

    for event in events:
        formatted = (
            f"[{event['stream_id']}:{event['stream_position']}] "
            f"{event['event_type']} -> {event['payload']}"
        )
        print(formatted)
        sys.stdout.flush()
        time.sleep(delay)

    elapsed = time.time() - start_time
    print(f"Total time elapsed: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()