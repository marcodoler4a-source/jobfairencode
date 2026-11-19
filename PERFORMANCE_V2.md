# Performance v2 — safe backend changes

Implemented for `/api/job-fair/sprs-google`:
- 2-minute TTL cache of Google Sheets responses; max 250 entries.
- Concurrent identical Google Sheets requests are deduplicated.
- 20-second upstream timeout.
- Reuses report builds for identical province/month requests (TTL, 20 entries).
- Opt-in server-side pagination (`page`, `pageSize` capped at 200), case-insensitive search (`search`), and server-side summary (`summary=true`).
- No query options: legacy response fields and full-record behavior preserved.

Example: `/api/job-fair/sprs-google?province=CAVITE&month=ALL&page=1&pageSize=50&search=sample`
Example counts: `/api/job-fair/sprs-google?province=CAVITE&month=ALL&summary=true`

IMPORTANT: This does not yet migrate every other HTML page or Google Apps Script endpoint to paginated requests. Those pages continue to load data as before. To realize smaller browser payloads, update each frontend to request pages and display the returned total. Google Sheets data may be up to 2 minutes stale; cache is per Render process and is cleared on restart. Existing save/update handlers are not changed. Validate the API against live sheets before production rollout.
