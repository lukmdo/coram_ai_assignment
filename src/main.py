import time
from typing import *
from contextlib import contextmanager
from collections import defaultdict

import psycopg2.extras
import sqlalchemy as sa
import sqlalchemy.exc as saError

DB_CONN_URI = "postgresql://postgres:postgres@postgres:5432/postgres"
#DB_CONN_URI = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
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


def ingest_events(conn: sa.Connection, events: Iterable[Event], bulk_size: int = DB_INGEST_BULK_SIZE) -> None:

    with conn.connection.cursor() as c:
        psycopg2.extras.execute_values(c,
            "INSERT into events(time, type) VALUES %s",
            events,
            page_size=bulk_size,
        )

def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    return {
        "people": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:04:00", "2023-08-10T10:05:00"),
        ],
        "vehicles": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:05:00", "2023-08-10T10:07:00"),
        ],
    }


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
        ingest_events(conn, events)

        aggregate_results = aggregate_events(conn)
        print(aggregate_results)


if __name__ == "__main__":
    main()
