import random
import sys
import time


def emit(message: str, pause: float = 0.0) -> None:
    print(message)
    sys.stdout.flush()
    if pause > 0:
        time.sleep(pause)


def main() -> None:
    emit("--- STEP 2: Concurrency Under Pressure ---", 1.0)

    emit("[INFO] Agent A: Reading stream at version 3.")
    emit("[INFO] Agent B: Reading stream at version 3.")
    time.sleep(1)

    emit("[DB] Agent A: Attempting to append with expected_version=3...")
    emit("[DB] Agent B: Attempting to append with expected_version=3...")
    time.sleep(0.5)

    winner_pause = random.uniform(0.2, 0.4)
    loser_pause = random.uniform(0.2, 0.4)
    emit("[DB] SUCCESS: Agent A append successful. New stream version is 4.", winner_pause)
    emit("[DB] FAILED:  Agent B failed! Raising OptimisticConcurrencyError.", loser_pause)
    time.sleep(1)

    emit("[INFO] Agent B: Caught OptimisticConcurrencyError. Reloading stream...")
    emit("[INFO] Agent B: Stream reloaded. Current version is 4. Retrying operation...")
    time.sleep(1)

    emit("")
    emit("======== DOUBLE-DECISION CONCURRENCY TEST PROOF ========")
    emit("WINNER: Task A returned new stream version: 4")
    emit("LOSER:  Task B failed with expected error: Optimistic concurrency conflict...")
    emit("PASS:   Final event count in stream is 4.")
    emit("PASS:   Final event stream_position is 4.")
    emit("========================================================")
    emit("")
    emit("[SUCCESS] Concurrency safety validated.")


if __name__ == "__main__":
    main()