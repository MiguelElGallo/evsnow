# Tutorial

Start here when you want to run EvSnow for the first time.

## Path

1. Create or verify the Snowflake objects.
2. Create or verify the Event Hub objects.
3. Run one Event Hub into one Snowflake target.
4. Prove rows arrived with a unique `run_id`.

## First Run Complete

The tutorial is complete only when:

1. The receiver starts and logs `Starting to receive messages`.
2. The sender publishes a uniquely tagged three-message batch.
3. The Snowflake query shows `rows_arrived = 3` and
   `missing_sequence_count = 0`.

## Pages

- [First run](first-run.md)
