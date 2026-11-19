# Performance improvements (safe first pass)

- Compressed large static HTML files with gzip using Node built-in zlib. Compression is cached per file and invalidated by file modification time/size. This reduces page transfer size, but does not accelerate Google Sheets reads.
- Added short browser caching for static image/script/style assets. HTML is revalidated.
- No UI, spreadsheet, report, form, or API business logic changed.
- Existing node_modules omitted from ZIP; Render installs dependencies using package.json/package-lock.json.

## Next step for major data speedups
Profile API endpoints and Google Sheets round trips, then add per-endpoint caching with invalidation on writes and server-side pagination. These changes require checking data consistency and should not be applied indiscriminately.
