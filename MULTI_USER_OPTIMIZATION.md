# Multi-user optimization (safe incremental update)

Changes to the existing SPRS Google Sheets route:
- Global upstream concurrency limit (`SPRS_UPSTREAM_LIMIT`, default 6) to avoid a flood of simultaneous Google Sheets requests when many visitors open reports.
- Existing per-sheet request deduplication, 120-second bounded cache, server-side pagination, and summary response are preserved.
- Cached search index for each assembled province report avoids repeatedly serializing every row for each search request; nested fields are searchable.
- Existing frontend SPRS behavior and full-result response are preserved, so no existing report/export behavior is intentionally changed.

## Operational notes
- This is **not** a site-wide migration to Redis, PostgreSQL, or server-side pagination across all modules. SPRS frontend still requests the legacy full dataset; the paginated API is available but not wired into the existing table UI.
- The upstream queue is process-local; use one Render instance unless a shared cache and coordinated upstream rate limiting are added.
- `SPRS_CACHE_TTL_MS` defaults to 120000; `SPRS_UPSTREAM_LIMIT` defaults to 6. Keep the latter low when experiencing Google throttling.
- Cache is eventually consistent for external Google Sheet edits, up to the configured TTL. Confirm freshness requirements before increasing TTL.
- Validate all pages and save/update flows in a staging Render deployment. No live Sheets or load test has been performed.
