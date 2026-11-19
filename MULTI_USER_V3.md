# Multi-user performance v3: shared spreadsheet reads within one instance

## Implemented
- Google Visualization (`docs.google.com/spreadsheets/d/.../gviz/tq`) GET read cache across backend modules, with 45-second default TTL.
- Concurrent identical reads share one upstream request, while each caller receives a separate Fetch Response.
- Limits concurrent upstream spreadsheet reads to 6 by default to reduce bursts from 50–100 users.
- Removes timestamp-only cache busters from cache keys so identical data requests can be reused.
- Bounds the cache to 32 MiB and 120 entries, and skips caching exceptionally large individual responses.
- Successful POST/PUT/PATCH/DELETE requests invalidate the spreadsheet read cache; pending older requests cannot repopulate the cache after invalidation.
- Preserves existing endpoint response formats, existing front-end files, and the earlier SPRS pagination API.

## Render environment variables (optional)
- SHEET_READ_TTL_MS=45000
- SHEET_READ_CONCURRENCY=6
- SHEET_READ_MAX_BYTES=33554432

## Important limits and next steps
- This is **per-instance memory caching**, not shared Redis caching. If Render uses more than one instance, each has its own cache.
- Only direct Google Visualization reads are optimized. Google Apps Script reads and private API calls are unchanged to avoid unsafe caching of private data.
- Website-wide frontend pagination is **not implemented**. Existing SPRS paginated API remains available, but legacy callers still download full responses.
- Dashboard computations and other module-specific pagination are not rewritten; these require endpoint and frontend-by-frontend integration to preserve existing calculations and exports.
- Writes through external Google Sheets interfaces will be visible after cache expiry, not instantly.
- Verify functionality with real Sheets and run load tests in a staging Render service before deploying to production.
