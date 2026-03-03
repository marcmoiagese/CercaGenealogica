# Nginx + proxy-aware config

Aquest directori conté un exemple de Nginx per posar Cercagenealogica darrere d’un reverse proxy.

## Configuració de l’app

Assegura aquests valors a `cnf/config.cfg` (o equivalents en producció):

- `PUBLIC_BASE_URL` (ex: `https://genealogia.cat`)
- `TRUSTED_ORIGINS` (ex: `https://genealogia.cat,http://localhost:8080`)
- `TRUSTED_PROXY_CIDRS` (ex: `127.0.0.1/32,::1/128,10.0.0.0/8`)

Això habilita:
- validació d’origen per rutes sensibles,
- IP real darrere de proxy,
- cookies segures quan el TLS acaba al proxy (`X-Forwarded-Proto`).

## Docker

- El proxy (Nginx/Traefik) ha d’enviar `X-Forwarded-*`.
- `TRUSTED_PROXY_CIDRS` ha d’incloure la xarxa interna del container (ex: `172.18.0.0/16`).

## Kubernetes

- L’Ingress ha d’enviar `X-Forwarded-For`, `X-Forwarded-Proto` i `X-Forwarded-Host`.
- `TRUSTED_PROXY_CIDRS` ha d’incloure el CIDR del node o del LB.

Consulta `cercagenealogica.conf.example` per veure els headers recomanats i el rate limit bàsic.
