# Tests del projecte Genealogia.cat

Aquesta carpeta conté **només codi de test**. L'aplicació en producció no depèn de cap fitxer aquí dins.

## Estructura

- `tests/unit/`
  - Tests unitaris de components petits (hash de contrasenya, validacions, helpers de BD, etc.)
- `tests/integration/`
  - Tests d'integració HTTP i fluxos complets (registre + login, perfil, canvi de contrasenya...)
- `tools/test.sh`
  - Script estàndard per executar tota la suite de tests.

## Execució

Des de l'arrel del projecte:

```bash
./tools/test.sh
```

Això executa:

```bash
go test ./... -cover -count=1
```

## Integració amb Codex / IA

- Després de qualsevol canvi de codi, la IA hauria d'executar sempre `./tools/test.sh`.
- Si algun test falla:
  - No s'ha de considerar el canvi com a "bo".
  - Cal corregir codi o test fins que la suite torni a passar.
