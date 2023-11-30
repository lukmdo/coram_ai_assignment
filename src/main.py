import datetime
import json
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import *

import psycopg2.extras
import sqlalchemy as sa
import sqlalchemy.exc as saError

DB_CONN_URI = "postgresql://postgres:postgres@postgres:5432/postgres"
# DB_CONN_URI = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
DB_INGEST_BULK_SIZE = 1000

Timestamp = NewType('Timestamp', str)
EventType = NewType('EventType', str)
type Event = tuple[Timestamp, EventType]


@contextmanager
def database_connection():
    conn = _database_connection()
    try:
        yield conn
    finally:
        conn.close()


def _database_connection() -> sa.Connection:
    engine = sa.create_engine(
        DB_CONN_URI,
        execution_options={"isolation_level": "AUTOCOMMIT"}
    )

    for attempt in range(5):
        try:
            conn = engine.connect()
        except saError.OperationalError as e:
            if attempt == 4:
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events"
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


def ingest_events(conn: sa.Connection, events: Iterable[Event],
                  bulk_size: int = DB_INGEST_BULK_SIZE) -> None:
    with conn.connection.cursor() as cx:
        psycopg2.extras.execute_values(
            cx,
            "INSERT into events(time, type) VALUES %s",
            events,
            page_size=bulk_size,
        )


def aggregate_events(conn: sa.Connection, begin: Timestamp, end: Timestamp) -> \
dict[str, list[tuple[Timestamp, Timestamp]]]:

    sql = """
WITH tmp_begins AS (
  SELECT
    row_number() over () as id,
    new_type,
    time as time_start
    FROM (
      SELECT
        time,
        CASE
          WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
          WHEN type IN ('van', 'car', 'truck') THEN 'vehicles'
        END as new_type,
        LAG(time) OVER (PARTITION BY (
          CASE
            WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
            WHEN type IN ('van', 'car', 'truck') THEN 'vehicles'
          END
        ) ORDER BY time) as l
      FROM events
      WHERE
        time BETWEEN :time_begin AND :time_end
    ) as x
  WHERE
    l IS NULL OR time - l > INTERVAL '1 minute'
  ORDER BY
    new_type, time
  ), tmp_ends AS (
  SELECT
    row_number() over () as id,
    new_type,
    time as time_end
  FROM (
    SELECT
      time,
      CASE
        WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
        WHEN type IN ('van', 'car', 'truck') THEN 'vehicles'
      END as new_type,
      LEAD(time) OVER (PARTITION BY (
        CASE
          WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
          WHEN type IN ('van', 'car', 'truck') THEN 'vehicles'
        END
      ) ORDER BY time) as l
    FROM events
    WHERE
      time BETWEEN :time_begin AND :time_end
  ) as x
  WHERE
    l IS NULL OR l - time > INTERVAL '1 minute'
  ORDER BY
    new_type, time
  )

SELECT * FROM tmp_begins
NATURAL LEFT JOIN  tmp_ends
"""
    results = conn.execute(
        sa.text(sql),
        {"time_begin": begin, "time_end": end},
    )

    response = defaultdict(list)
    _ = response["people"]
    _ = response["vehicles"]


    for row in results:
        response[row[1]].append((row[2], row[3]))

    return response


def _json_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        obj = obj.replace(tzinfo=None)
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def main():
    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    with database_connection() as conn:
        # ingest_events(conn, events)

        results = aggregate_events(
            conn,
            begin="2023-08-10",
            end="2023-08-11",
        )
        print(
            "aggregate_events",
            json.dumps(results, indent=2, sort_keys=True, default=_json_serializer)
        )


if __name__ == "__main__":
    main()
