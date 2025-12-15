# Lògica web (core)

El paquet `core/` conté el servidor HTTP, la renderització de plantilles, la seguretat bàsica i els handlers de negoci.

## Components principals

- `app.go`: estructura `App` (config + DB + mail) i utilitats compartides.
- `templates.go`: càrrega de plantilles i helpers (`t` per i18n, `default`, etc.).
- `i18n.go`: càrrega de `locales/*.json` i resolució d’idioma.
- `csrf.go`: CSRF amb patró *double-submit* (cookie `cg_csrf` + token al formulari).
- `webserver.go`: middlewares de seguretat (bloqueig IP, rate limit, static files controlats).
- `permissions.go`: càlcul i verificació de permisos (polítiques) per a rutes d’admin.

## Idiomes (i18n)

- Fitxers: `locales/cat.json`, `locales/en.json`, `locales/oc.json`.
- Resolució d’idioma:
  1. Cookie `lang`
  2. Header `Accept-Language`
  3. Fallback a `cat`
- A la zona privada, si l’usuari té `PreferredLang`, aquest idioma té prioritat.

## Seguretat (resum)

- **CSRF**: cookie HttpOnly + token en formulari, validació constant-time.
- **Rate limiting**: token bucket per ruta amb configuració diferent per `/login`, `/registre` i `/static/`.
- **Bloqueig per IP**: via `BLOCKED_IPS` al `config.cfg`.
- **Static files**: evita path traversal, evita llistat de directoris i filtra rutes autoritzades.

## Permisos / polítiques

- Els permisos efectius es calculen a partir de polítiques assignades a usuari i/o grup.
- En entorns buits (sense cap política), el codi dona permisos amplis per defecte per facilitar desenvolupament.

## Fluxos implementats

- Registre + activació (token)
- Login + sessió (`cg_session`)
- Recuperació de contrasenya
- Gestió de perfil i privacitat
- Public user profile (`/u/...`) i ranking
- CRUD i administració d’entitats (territori, eclesiàstic, documentals, etc.) segons permisos
- Moderació (aprovar/rebutjar)
