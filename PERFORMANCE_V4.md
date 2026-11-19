# Performance v4 — optional shared Redis caching

## Implemented
- Optional Redis-compatible shared caching of Google Visualization spreadsheet GET responses, across Render instances.
- Shared generation key invalidation on successful API writes, so instances do not keep using their old shared cache after a write.
- Existing local cache, request deduplication and upstream concurrency limiter retained.
- Fallback to existing in-process caching when REDIS_URL is not set or Redis is unavailable.
- Existing SPRS server-side pagination and summary endpoint retained unchanged.

## Setup
1. Provision a Redis-compatible Render Key Value instance.
2. Set `REDIS_URL` on the Render web service to its internal connection URL.
3. Deploy and check logs for `Shared Redis sheet cache connected`.
4. Keep `SHEET_READ_TTL_MS=45000` and `SHEET_READ_CONCURRENCY=6` as initial settings.
5. Run `npm install` to update dependencies and lockfile before committing, if your deployment uses npm ci.

## Important limitations
- **Website-wide frontend pagination has not been implemented.** Each module has distinct rendering and export logic and needs dedicated integration and testing.
- **Dashboard calculation restructuring has not been implemented.** Existing SPRS summary calculations are retained.
- Redis currently covers only the same public Google Visualization reads covered by v3; private Apps Script calls are deliberately not shared-cached.
- Cache invalidation is triggered by successful API writes; writes directly to Sheets or outside this backend become visible after cache expiry.
- Redis cache deduplication across instances is not distributed locking; two instances can still miss simultaneously.
- Redis should use private network access and credentials; never expose REDIS_URL to frontend code.
- This build passed `node --check`; live Google Sheets and multi-user testing have not been performed.
