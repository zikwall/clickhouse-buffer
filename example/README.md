### Example

```shell
export CLICKHOUSE_HOST=111.11.11.11:9000
export CLICKHOUSE_USER=111.11.11.11:9000
export REDIS_HOST=111.11.11.11:6379
export REDIS_PASS=password_if_needed
```

- `$ go run ./cmd/simple/main.go`
- `$ go run ./cmd/redis/main.go`

```shell
clickhouse-client -h<host>

SELECT
    id,
    uuid,
    insert_ts
FROM default.example
ORDER BY id ASC

Query id: 074f42ff-0ea7-44ca-9cd1-735e8fb5ce54

┌─id─┬─uuid─────┬─insert_ts───────────┐
│  1 │ uuidf 1  │ 09 Jun 22 13:42 MSK │
│  2 │ uuidf 2  │ 09 Jun 22 13:42 MSK │
│  3 │ uuidf 3  │ 09 Jun 22 13:42 MSK │
│  4 │ uuidf 4  │ 09 Jun 22 13:42 MSK │
│  5 │ uuidf 5  │ 09 Jun 22 13:42 MSK │
│  6 │ uuidf 6  │ 09 Jun 22 13:42 MSK │
│  7 │ uuidf 7  │ 09 Jun 22 13:42 MSK │
│  8 │ uuidf 8  │ 09 Jun 22 13:42 MSK │
│  9 │ uuidf 9  │ 09 Jun 22 13:42 MSK │
│ 10 │ uuidf 10 │ 09 Jun 22 13:42 MSK │
└────┴──────────┴─────────────────────┘

10 rows in set. Elapsed: 0.015 sec.
```

- `$ go run ./cmd/advanced/main.go`
- `$ go run ./cmd/advanced_redis/main.go`

```sql
SELECT * FROM default.advanced_example;
```