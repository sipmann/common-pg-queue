# common-pg-queue

Fila de jobs duravel sobre Postgres pro boilerplate (`component` +
`msolli/proletarian`), no mesmo espirito do `common-rabbitmq` - reusa o
Postgres que a app ja tem (sem serviço extra numa VPS pequena).

## Uso

```clojure
(component/system-map
  :config (config/new-config)
  :database (component/using (database/new-database) [:config])
  :queue-producer (component/using (producer/new-producer) [:database])
  :email-worker (component/using
                 (consumer/new-queue-worker :emails handlers/handle-job!)
                 [:database]))
```

```clojure
(producer/enqueue! queue-producer ::send-email {:to "cliente@exemplo.com"})
```

Em testes, troque `producer`/`consumer` por `producer-mock`/`consumer-mock`
(mesma API, sem tocar em Postgres).

## Build

```
clj -T:build clean
clj -T:build jar
clj -T:build install
```

## TODOs
- LISTEN/NOTIFY como otimizacao de latencia por cima do polling (opcional,
  decidido explicitamente pra depois - ver debate no system-updater)
- Testes de integracao contra Postgres embutido (pg-embedded-clj)
- Schema de payload por job-type
