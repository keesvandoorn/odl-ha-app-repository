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
