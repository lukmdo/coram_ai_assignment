# Video events management task

This repo contains a short interview coding task at Coram.AI

You are asked to implement backend APIs of a video event management system supporting basic operations of:
1. Ingestion of new video events.
2. Aggregation of recorded video events.
3. Alert triggered on unusual activity.

The events are stored in postgres database the backend interacts with. In our case backend are simple python functions.

## Setup
1. Install [docker](https://docs.docker.com/engine/install/) and [docker-compose](https://docs.docker.com/compose/install/)
2. Run the code using `./run.sh`. This script starts the database and runs implemented APIs.

## Tasks
All tasks should be implemented within `main.py` file:

### Ingestion
To start off, please complete the `ingest_data` function - feel free to change the signature if you want to. Please also consider the implications of the code - performance, maintenance, etc.
The events have a "time" and a "type" property. The "type" will always be one of a set of pre-defined strings - "pedestrian", "bicycle", "car", "truck", "van". Assume the events are ordered.

My Notes:
```shell
# server
docker run -it --rm -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres postgres:13.7-alpine
# cli
docker run -it --network host -e PGPASSWORD=postgres --rm postgres:13.7-alpine psql -h 127.0.0.1 -U postgres
```
- required server-side values binding (security and performance for bulk)
- ideally would single `prepare` with multiple `bind` (each in bulk) however `psycopg2` does not supprt it ( maybe good reason )
- tests skipped and used e2e test to keep 2h task limit

### Aggregation
We'd like to show our user an overview of the events in the database. To do that please complete the "aggregate_events" function, so that it returns **two lists of periods of activity**. One should contain the activity of people who were detected (the "pedestrian" and "bicycle" event types) and the other should contain the activity of vehicles (the "car", "truck" and "van" event types).
The goal is to aggregate the events in the database into periods of activity in each category. Any events less than 1 minute apart in the same category should be combined into an interval.

For example if we assume that the system has detected cars at the following timestamps: 18:30:30, 18:31:00, 18:31:30, 18:35:00, 18:35:30, 18:36:00, 18:37:30, 18:38:00. The aggregate function should return three periods of activity: 18:30:30-18:31:30, 18:35:00-18:36:00 and 18:37:30-18:38:00.

```
{
    "people": [],
    "vehicles": [
        ("2023-08-10T18:30:30", "2023-08-10T18:31:30"),
        ("2023-08-10T18:35:00", "2023-08-10T18:36:00"),
        ("2023-08-10T18:37:30", "2023-08-10T18:38:00"),
    ],
}
```
**Please have the whole aggregation logic in SQL.**

My Notes:
- I see how centralized aggregation on DB could be handy
  - background aggregate and store separately
- I [created SQL 👾](https://github.com/search?q=repo%3Alukmdo%2Fcoram_ai_assignment%20%22def%20aggregate_events%22&type=code) that I don't even dare to `EXPLAIN ...`
  - learned [`LAG/LEAD() OVER()`](https://www.postgresql.org/docs/13/functions-window.html)
  - refreshed my SQL unions/joins and found
```
SELECT * FROM (VALUES (1, 2), (10, 20)) as x(a, b)
```

### Alerts
The last part is related to real-time alerts for unusual events. Our users would like to be notified if a person is detected for a continued period of time. Imagine that the ingestion function is called real-time as the detections happen every 30 seconds.

Please add additional logic to the ingestion function which will print to the console if a person is detected in 5 consecutive events. Please consider performance - the ingestion function will be called very often.

My Notes:
Ambiguous for me so I assert:
- can rely on events being sorted
- alert only for "pedestrian"
- using only data to ingest - no lookups / memoization
- consecutive events are spaced 30s apart ( bigger gap will split )
- one alert per ingest is enough ( no significant difference )
- modified `ingest_events(conn, events)` is still OK
- keeping `ingest_events(conn, event)`
- `maybe_alert` not tested (ex. 2h limit)
