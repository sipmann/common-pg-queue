# common-pg-queue

Durable job queue on top of Postgres for the boilerplate (`component` +
`msolli/proletarian`), in the same spirit as `common-rabbitmq` - reuses
the Postgres the app already has (no extra service on a small VPS).

## Usage

```clojure
(component/system-map
  :config (config/new-config)
  :database (component/using (database/new-database) [:config])
  :queue-producer (component/using (producer/new-producer) [:database])
  :queue-workers (component/using
                  (consumer/new-queue-worker-manager
                   [{:queue :emails :handler handlers/handle-job!}
                    {:queue :thumbnails :handler handlers/handle-job!}])
                  [:database]))
```

```clojure
(protocols/enqueue! queue-producer ::send-email {:to "customer@example.com"})
```

`producer`/`producer-mock` both implement `protocols/JobProducer`, so
`protocols/enqueue!` works the same on either - swap in `producer-mock`
for tests without touching Postgres and without changing call sites.

## Build

```
clj -T:build clean
clj -T:build jar
clj -T:build install
```

## TODOs
- LISTEN/NOTIFY as a latency optimization on top of polling (optional,
  explicitly deferred - see the debate in system-updater)
- Integration tests against embedded Postgres (pg-embedded-clj)
- Payload schema per job-type
