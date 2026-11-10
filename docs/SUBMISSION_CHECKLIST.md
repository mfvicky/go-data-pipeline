# Submission Checklist — Go Data Processing Pipeline

- [ ] Use Go 1.27; `make deps` and `make ci` pass, including vet, lint, unit tests, build and race detection.
- [ ] `docker compose ps` shows API, PostgreSQL, RabbitMQ healthy and Prometheus up; verify `/health`, `/ready` and `/metrics`.
- [ ] Run the Postman collection and verify its tests have zero failures; include `PATCH /cancel` and `DELETE` operations.
- [ ] Run `examples/verify_global_daily.ps1` on Docker; verify three joined UTC dates and GORM/PostgreSQL export rows.
- [ ] Run `examples/verify_global_daily.ps1 -RestartApi -DeleteAfterCheck` to verify persistent files and correct cleanup. Use a fresh sample job; this deletes only that test job.
- [ ] README, `docs/architecture.md`, `docs/design-decisions.md`, `docs/TECHNICAL_REPORT.md` (≤ one page), and `docs/openapi.yaml` describe the current GORM + RabbitMQ implementation.
- [ ] Included source fixtures and sample payloads are synthetic/public and contain no real credentials.
- [ ] Exclude `.env`, `prometheus-secrets/api_key`, generated output, temporary files, and caches. Rotate any previously exposed key.
- [ ] Check `git diff --check`, `git status --short`, commit your changes and push the correct branch.
- [ ] Inspect hosted GitHub Actions results; local success does not prove remote CI passed.

Known limitations: single API replica, at-least-once broker delivery, non-atomic DB/filesystem cleanup, full record snapshots in memory/JSONB, optional TLS requires a real deployment, and multi-tenant authorization is not provided.
