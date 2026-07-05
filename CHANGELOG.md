## v0.4.0 - 2026-07-05

- Gateway POST-client toegevoegd.
- `gateway_enabled` optie toegevoegd, standaard uit.
- `collector_token` wordt alleen gebruikt voor Gateway POST en nooit gelogd.
- Gateway POST-counters toegevoegd aan statusbestand.
- Lifecycle heartbeat/start/stop records voorbereid voor Gateway-route.
- Geen buffering/retry/deadletter in deze versie.

# Changelog

## 0.1.2

- `hassio_role` aangepast van `default` naar `homeassistant`.
- Doel: minimale Supervisor-rol voor lezen van Home Assistant Core logs.
- Geen `homeassistant_api`.
- Geen `docker_api`.
- Geen Gateway POST.
- Geen logreader.

## 0.1.1

- run.sh aangepast naar `/usr/bin/with-contenv bash`.
- Doel: Supervisor runtime environment beschikbaar maken voor `SUPERVISOR_TOKEN`.
- Geen scope-uitbreiding.
- Geen Gateway POST.
- Geen logreader.

## 0.1.0

- Eerste repositoryversie.
- App-skelet voor ODL ha-vm Connector.
- Supervisor Core logs als bronroute voorbereid.
- Geen Gateway POST.
- Geen logverwerking.
- Geen collector-token vereist.
