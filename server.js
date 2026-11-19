// Merged Express Backend with Google Sheets/Drive Workflow Parity
    // + First-login email setup + OTP Forgot Password
    // + SPRS / Consolidated Job Fair Reports
    // + PUBLIC JSON DATA STORAGE
    // ============================================================================

    const express = require('express');
    const fs = require('fs');
    const path = require('path');
    const crypto = require('crypto');


    // Multi-user read optimization. Only public Google Visualization spreadsheet
    // GET responses are cached; authentication, private Apps Script endpoints,
    // POST requests and other Google APIs always use the original fetch.
    const nativeFetch = globalThis.fetch.bind(globalThis);
    // Optional Redis-compatible shared cache for multiple Render instances.
    // Falls back to existing bounded in-process cache if REDIS_URL is unset.
    let sharedRedis = null;
    let sharedRedisReady = false;
    let sharedGeneration = '0';
    if (process.env.REDIS_URL) {
      try {
        const { createClient } = require('redis');
        sharedRedis = createClient({ url: process.env.REDIS_URL, socket: { reconnectStrategy: retries => Math.min(3000, 100 * 2 ** Math.min(retries, 5)) } });
        sharedRedis.on('error', err => console.warn('Shared cache unavailable:', err.message));
        sharedRedis.connect().then(async () => {
          sharedRedisReady = true;
          sharedGeneration = (await sharedRedis.get('epms:sheet:generation')) || '0';
          console.log('Shared Redis sheet cache connected');
        }).catch(err => console.warn('Redis connection failed:', err.message));
      } catch (err) { console.warn('Redis disabled:', err.message); }
    }
    const sharedKey = (generation, key) => 'epms:sheet:v4:' + generation + ':' + crypto.createHash('sha256').update(key).digest('hex');
    async function sharedRead(key) {
      if (!sharedRedisReady) return null;
      try {
        const generation = (await sharedRedis.get('epms:sheet:generation')) || '0';
        if (generation !== sharedGeneration) { sharedGeneration = generation; sheetReadCache.clear(); sheetReadBytes = 0; }
        const value = await sharedRedis.get(sharedKey(generation, key));
        return value ? JSON.parse(value) : null;
      } catch { return null; }
    }
    async function sharedWrite(key, value, generation) {
      if (!sharedRedisReady) return;
      try {
        const current = (await sharedRedis.get('epms:sheet:generation')) || '0';
        if (current !== generation) return;
        await sharedRedis.set(sharedKey(generation, key), JSON.stringify(value), { EX: Math.max(5, Math.ceil(SHEET_READ_TTL_MS / 1000)) });
      } catch (err) { console.warn('Redis cache write failed:', err.message); }
    }
    const sheetReadCache = new Map();
    const sheetReadPending = new Map();
    const SHEET_READ_TTL_MS = Math.max(5000, Number(process.env.SHEET_READ_TTL_MS) || 45000);
    const SHEET_READ_MAX_BYTES = Math.max(1048576, Number(process.env.SHEET_READ_MAX_BYTES) || 32 * 1048576);
    const SHEET_READ_MAX_ITEMS = 120;
    let sheetReadBytes = 0;
    let sheetCacheGeneration = 0;
    let sheetActive = 0;
    const sheetWaiters = [];
    const SHEET_READ_CONCURRENCY = Math.max(1, Math.min(20, Number(process.env.SHEET_READ_CONCURRENCY) || 6));
    const sheetStats = { hits: 0, misses: 0, deduped: 0, upstream: 0, failures: 0 };
    function clearSheetReadCache() {
      sheetCacheGeneration++;
      sheetReadCache.clear();
      sheetReadBytes = 0;
      if (sharedRedisReady) {
        sharedRedis.incr('epms:sheet:generation').then(v => { sharedGeneration = String(v); })
          .catch(err => console.warn('Redis cache invalidation failed:', err.message));
      }
    }
    function dropSheetEntry(key) {
      const entry = sheetReadCache.get(key);
      if (entry) sheetReadBytes -= entry.size;
      sheetReadCache.delete(key);
    }
    async function withSheetSlot(fn) {
      if (sheetActive >= SHEET_READ_CONCURRENCY) await new Promise(resolve => sheetWaiters.push(resolve));
      sheetActive++;
      try { return await fn(); }
      finally { sheetActive--; const next = sheetWaiters.shift(); if (next) next(); }
    }
    function isCacheableSheetRead(input, options) {
      if (typeof input !== 'string') return false;
      if (options && options.method && String(options.method).toUpperCase() !== 'GET') return false;
      try {
        const url = new URL(input);
        return url.protocol === 'https:' && url.hostname === 'docs.google.com' &&
          /^\/spreadsheets\/d\/[^/]+\/gviz\/tq$/.test(url.pathname);
      } catch { return false; }
    }
    // Use a normalized URL as cache key; Google Sheets _=timestamp is only a
    // cache buster and does not change the requested data.
    function sheetKey(input) {
      const url = new URL(input);
      url.searchParams.delete('_');
      url.searchParams.sort();
      return url.toString();
    }
    globalThis.fetch = async function optimizedFetch(input, options) {
      if (!isCacheableSheetRead(input, options)) return nativeFetch(input, options);
      // Caller-provided abort signals need their own upstream lifecycle.
      // Deduplication is safe for these reads because all consumers receive a
      // distinct Response object; upstream abort only affects the initial fetch.
      const key = sheetKey(input);
      const now = Date.now();
      // Shared cache is checked first so invalidations from other instances are respected.
      const shared = await sharedRead(key);
      if (shared) {
        sheetStats.hits++;
        return new Response(shared.body, {status: shared.status, headers: shared.headers});
      }
      const entry = sharedRedisReady ? null : sheetReadCache.get(key);
      if (entry && entry.expires > now) {
        sheetStats.hits++;
        return new Response(entry.body, {status: entry.status, headers: entry.headers});
      }
      if (entry) dropSheetEntry(key);
      if (sheetReadPending.has(key)) {
        sheetStats.deduped++;
        const value = await sheetReadPending.get(key);
        return new Response(value.body, {status: value.status, headers: value.headers});
      }
      sheetStats.misses++;
      const generation = sheetCacheGeneration;
      const redisGeneration = sharedGeneration;
      const pending = withSheetSlot(async () => {
        sheetStats.upstream++;
        // Strip the timestamp cache-buster so requests with identical contents
        // have the same upstream URL. Preserve all meaningful query parameters.
        const response = await nativeFetch(key, options);
        const body = await response.text();
        const value = {body, status: response.status, headers: {'content-type': response.headers.get('content-type') || 'text/plain'}};
        if (response.ok && generation === sheetCacheGeneration && !body.includes('"status":"error"')) {
          const size = Buffer.byteLength(body, 'utf8');
          if (size <= SHEET_READ_MAX_BYTES / 2) {
            if (sharedRedisReady) await sharedWrite(key, value, redisGeneration);
            const old = sheetReadCache.get(key);
            if (old) dropSheetEntry(key);
            sheetReadCache.set(key, {...value, size, expires: Date.now() + SHEET_READ_TTL_MS});
            sheetReadBytes += size;
            while (sheetReadBytes > SHEET_READ_MAX_BYTES || sheetReadCache.size > SHEET_READ_MAX_ITEMS) {
              dropSheetEntry(sheetReadCache.keys().next().value);
            }
          }
        }
        return value;
      });
      sheetReadPending.set(key, pending);
      try {
        const value = await pending;
        return new Response(value.body, {status: value.status, headers: value.headers});
      } catch (err) { sheetStats.failures++; throw err; }
      finally { if (sheetReadPending.get(key) === pending) sheetReadPending.delete(key); }
    };

    // ============================================================================
    // SAFE IMPORTS FOR OPTIONAL LOCAL MODULES
    // ============================================================================
    let usersModule = {};
    let authModule = {};

    try {
      usersModule = require('./users');
    } catch (e) {
      usersModule = {
        getCurrentUserContext: (username) => {
          const u = String(username || 'DOLE4A').toUpperCase();

          return {
            username: u,
            name: u,
            role: u,
            office: u,
            fieldOffice: u
          };
        },

        isUserAuthorized: (email) => {
          const authorizedEmails = [
            "marco.doler4a@gmail.com",
            "ro4a_tssd.epww@dole.gov.ph",
            "tssdew.dole4a@gmail.com",
            "jobfairscalabarzon@gmail.com"
          ];

          return authorizedEmails.includes(
            String(email || '').toLowerCase()
          );
        }
      };
    }

    try {
      authModule = require('./auth');

      console.log('SUCCESS: auth.js loaded successfully.');

    } catch (e) {

      // IMPORTANT:
      // A require('./auth') failure does NOT always mean auth.js is missing.
      // It can also mean auth.js contains a syntax error, has a bad import,
      // or throws an error while loading. Log the REAL error so it can be fixed.
      console.error('================================================');
      console.error('ERROR LOADING auth.js');
      console.error(e && e.stack ? e.stack : e);
      console.error('================================================');

      const authLoadError =
        e && e.message
          ? e.message
          : String(e || 'Unknown auth.js loading error.');

      authModule = {
        authenticateUser: async () => ({
          success: false,
          message:
            'Authentication module (auth.js) failed to load: ' +
            authLoadError
        }),

        changeUserPassword: async () => ({
          success: false,
          message:
            'Authentication module (auth.js) failed to load: ' +
            authLoadError
        }),

        resetUserPassword: async () => ({
          success: false,
          message:
            'Authentication module (auth.js) failed to load: ' +
            authLoadError
        }),

        setUserPassword: async () => ({
          success: false,
          message:
            'Authentication module (auth.js) failed to load: ' +
            authLoadError
        })
      };
    }

    const {
      getCurrentUserContext,
      isUserAuthorized
    } = usersModule;

    const {
      authenticateUser,
      changeUserPassword,
      resetUserPassword,
      setUserPassword
    } = authModule;


    // ============================================================================
    // OPTIONAL EMAIL SUPPORT (nodemailer) — SAFE IMPORT
    // ============================================================================
    let nodemailer = null;

    try {
      nodemailer = require('nodemailer');
    } catch (e) {
      nodemailer = null;

      console.warn(
        "nodemailer package not found. Run 'npm install nodemailer' " +
        "to enable real email sending. Simulating email sends until then."
      );
    }

    let mailTransporter = null;

    function getMailTransporter() {
      if (mailTransporter) return mailTransporter;

      if (!nodemailer) return null;

      const host = process.env.EMAIL_HOST;
      const user = process.env.EMAIL_USER;
      const pass = process.env.EMAIL_PASS;

      if (!host || !user || !pass) {
        return null;
      }

      mailTransporter = nodemailer.createTransport({
        host,
        port: process.env.EMAIL_PORT
          ? parseInt(process.env.EMAIL_PORT, 10)
          : 587,
        secure: process.env.EMAIL_SECURE === 'true',
        auth: {
          user,
          pass
        }
      });

      return mailTransporter;
    }


    // ============================================================================
    // EXPRESS APP CONFIGURATION
    // ============================================================================
    const app = express();

    const PORT = process.env.PORT || 3000;


    // ============================================================================
    // GOOGLE DRIVE / SHEETS CONSTANTS
    // ============================================================================
    const SPREADSHEET_ID =
      "1-dN0cj7ozUlH14t7otsucxwDOOOeZ67WoWsQOGZ_aM4";

    const SHEET_NAME = "Sheet1";

    const PROOF_RECEIVED_FOLDER_ID =
      "19p6xdvq0TjPcPrWq5HkicjyWrgyg0U4z";

    const PROOF_RELEASED_FOLDER_ID =
      "1ksmyxvv-wp8sVUqh9U9oF7DQwmErQVPZ";

    const PROOF_RECEIVED_DRIVE_URL =
      "https://drive.google.com/drive/folders/19p6xdvq0TjPcPrWq5HkicjyWrgyg0U4z?usp=sharing";

    const PROOF_RELEASED_DRIVE_URL =
      "https://drive.google.com/drive/folders/1ksmyxvv-wp8sVUqh9U9oF7DQwmErQVPZ?usp=sharing";


    // After a successful application write, force the next sheet read to refresh.
    // No changes to any existing save handler or its response format.
    app.use((req, res, next) => {
      if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
        res.once('finish', () => { if (res.statusCode >= 200 && res.statusCode < 300) clearSheetReadCache(); });
      }
      next();
    });


// Read-only JF Database CSV proxy. Google Sheet gid 257088478, columns A:Y.
app.get('/api/jf/new-data-csv', async (req, res) => {
  const id = '1JS0EbUZl1A4KPjov18PCj7H_kJ4bnPv8lWG_Uo_SqLs';
  const gid = '257088478';
  const urls = [
    `https://docs.google.com/spreadsheets/d/${id}/export?format=csv&gid=${gid}`,
    `https://docs.google.com/spreadsheets/d/${id}/gviz/tq?tqx=out:csv&gid=${gid}&range=A:Y`
  ];
  const errors = [];
  for (const url of urls) {
    try {
      const upstream = await nativeFetch(url, {redirect:'follow', signal:AbortSignal.timeout(14000)});
      const csv = await upstream.text();
      if (!upstream.ok) throw new Error('Google HTTP ' + upstream.status);
      if (!csv.trim() || /^\s*(?:<!doctype|<html|<\?xml)/i.test(csv)) throw new Error('Google returned empty or sign-in HTML instead of CSV');
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).send(csv);
    } catch (e) { errors.push(e.message); }
  }
  return res.status(502).json({success:false, message:'Unable to read NEW DATA sheet. Check spreadsheet sharing and gid.', details:errors});
});

    // ============================================================================
    // CORS MIDDLEWARE
    // ============================================================================
    app.use((req, res, next) => {

      const allowedOrigins = String(
        process.env.ALLOWED_ORIGINS || '*'
      )
        .split(',')
        .map(v => v.trim())
        .filter(Boolean);

      const requestOrigin = req.headers.origin;

      if (
        allowedOrigins.includes('*') ||
        (requestOrigin && allowedOrigins.includes(requestOrigin))
      ) {
        res.header(
          'Access-Control-Allow-Origin',
          allowedOrigins.includes('*')
            ? '*'
            : requestOrigin
        );
      }

      res.header(
        'Access-Control-Allow-Methods',
        'GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD'
      );

      res.header(
        'Access-Control-Allow-Headers',
        'Origin, X-Requested-With, Content-Type, Accept, Authorization'
      );

      res.header(
        'Access-Control-Allow-Credentials',
        'true'
      );

      if (req.method === 'OPTIONS') {
        return res.sendStatus(204);
      }

      next();
    });


    // ============================================================================
    // GLOBAL BODY PARSERS
    // ============================================================================
    app.use(express.json({
      limit: '50mb'
    }));

    app.use(express.urlencoded({
      extended: true,
      limit: '50mb'
    }));



// Encode Employers Venue Management -> shared Google Sheet bridge.
// Configure VENUE_APPS_SCRIPT_URL on Render after deploying the companion Code.gs.
// IMPORTANT: Protect POST operations with your existing server-side admin authentication.
const VENUE_APPS_SCRIPT_URL = process.env.VENUE_APPS_SCRIPT_URL || '';
app.get('/api/jf/venue-management', async (req,res)=>{
  if(!VENUE_APPS_SCRIPT_URL) return res.status(503).json({success:false,message:'VENUE_APPS_SCRIPT_URL is not configured.'});
  try{
    const upstream=await fetch(VENUE_APPS_SCRIPT_URL+'?action=list',{redirect:'follow'});
    const data=await upstream.json();
    return res.status(upstream.ok?200:502).json(data);
  }catch(e){return res.status(502).json({success:false,message:e.message});}
});
app.post('/api/jf/venue-management', async (req,res)=>{
  if(!VENUE_APPS_SCRIPT_URL) return res.status(503).json({success:false,message:'VENUE_APPS_SCRIPT_URL is not configured.'});
  const action=String(req.body?.action||'');
  if(!['add','clear'].includes(action))return res.status(400).json({success:false,message:'Invalid action.'});
  try{
    const upstream=await fetch(VENUE_APPS_SCRIPT_URL,{
      method:'POST',headers:{'Content-Type':'text/plain;charset=utf-8'},
      redirect:'follow',body:JSON.stringify({action,venue:req.body?.venue||''})
    });
    const data=await upstream.json();
    return res.status(upstream.ok?200:502).json(data);
  }catch(e){return res.status(502).json({success:false,message:e.message});}
});

    // ============================================================================
    // SPES GOOGLE APPS SCRIPT BRIDGE
    // ============================================================================
    // Set SPES_APPS_SCRIPT_URL in your hosting environment to the deployed
    // Apps Script Web App /exec URL. SPES.html calls this same-origin endpoint,
    // so the browser does not need to call google.script.run.
    const SPES_APPS_SCRIPT_URL = String(
  process.env.SPES_APPS_SCRIPT_URL ||
  'https://script.google.com/macros/s/AKfycbzNgq-8HDyYInvNTQEUrbelFB7aRvUcoKGqkr_2u3axXiWGFD24PKVXKyqzJfneyF_N/exec'
).trim();

    const SPES_ALLOWED_ACTIONS = new Set([
      'getCurrentUserEmail',
      'getSpesSummaryReport',
      'getContractRawPledges',
      'updateRawPledge',
      'deleteRawPledge',
      'undoDeletedRawPledge',
      'getNextControlNumber',
      'saveRawPledge',
      'getFormsSettings',
      'verifyFormsAdminPassword',
      'saveFormsSettings',
      'saveEmployerReference',
      'updateEmployerReference',
      'getAllEmployers',
  'getEmployerDetails',
      'getEmployersByProvince',
      'getRecentRawPledges'
    ]);

    // SPES.html sends Content-Type: text/plain;charset=utf-8.
    // Parse that body here without changing any other API routes/functions.
    app.post('/api/spes', express.text({ type: 'text/plain', limit: '50mb' }), async (req, res) => {
      try {
        if (!SPES_APPS_SCRIPT_URL) {
          return res.status(500).json({
            success: false,
            message: 'SPES_APPS_SCRIPT_URL is not configured on the server.'
          });
        }

        // Accept BOTH:
        // 1) text/plain JSON sent by SPES.html
        // 2) application/json sent by other callers/tests
        let spesBody = req.body;

        if (typeof spesBody === 'string') {
          const rawSpesBody = spesBody.trim();
          if (!rawSpesBody) {
            return res.status(400).json({
              success: false,
              message: 'Empty SPES request body.'
            });
          }

          try {
            spesBody = JSON.parse(rawSpesBody);
          } catch (parseError) {
            return res.status(400).json({
              success: false,
              message: 'Invalid SPES request JSON: ' + parseError.message
            });
          }
        }

        if (!spesBody || typeof spesBody !== 'object' || Array.isArray(spesBody)) {
          spesBody = {};
        }

        // Normalize action names so older/newer HTML builds that send different
        // capitalization still resolve to the exact Apps Script function name.
        const rawAction = String(spesBody.action || '').trim();
        const SPES_ACTION_ALIASES = {
          getcurrentuseremail: 'getCurrentUserEmail',
          getspessummaryreport: 'getSpesSummaryReport',
          getcontractrawpledges: 'getContractRawPledges',
          updaterawpledge: 'updateRawPledge',
          deleterawpledge: 'deleteRawPledge',
          undodeletedrawpledge: 'undoDeletedRawPledge',
          getnextcontrolnumber: 'getNextControlNumber',
          saverawpledge: 'saveRawPledge',
          getformssettings: 'getFormsSettings',
          verifyformsadminpassword: 'verifyFormsAdminPassword',
          saveformssettings: 'saveFormsSettings',
          saveemployerreference: 'saveEmployerReference',
          updateemployerreference: 'updateEmployerReference',
          getallemployers: 'getAllEmployers',
          getemployerdetails: 'getEmployerDetails',
          getemployersbyprovince: 'getEmployersByProvince',
          getrecentrawpledges: 'getRecentRawPledges'
        };
        const action = SPES_ACTION_ALIASES[rawAction.toLowerCase()] || rawAction;
        const args = Array.isArray(spesBody.args) ? spesBody.args : [];
        const context = spesBody.context && typeof spesBody.context === 'object'
          ? spesBody.context
          : {};

        if (!SPES_ALLOWED_ACTIONS.has(action)) {
          return res.status(400).json({
            success: false,
            message: 'Unsupported SPES action: ' + action
          });
        }

        const upstream = await fetch(SPES_APPS_SCRIPT_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          redirect: 'follow',
          body: JSON.stringify({ action, args, context })
        });

        const textBody = await upstream.text();

        if (!upstream.ok) {
          console.error('SPES Apps Script HTTP error:', upstream.status, textBody.slice(0, 1000));
          return res.status(502).json({
            success: false,
            __error: 'SPES Apps Script HTTP ' + upstream.status,
            message: 'SPES Apps Script HTTP ' + upstream.status + ': ' + textBody.slice(0, 500)
          });
        }

        let data;
        try {
          data = JSON.parse(textBody);
        } catch (parseError) {
          console.error('SPES Apps Script non-JSON response:', textBody.slice(0, 1000));
          return res.status(502).json({
            success: false,
            __error: 'SPES Apps Script returned non-JSON.',
            message:
              'SPES Apps Script returned non-JSON. Verify that SPES_APPS_SCRIPT_URL points to the latest deployed /exec Web App and that access is set to Anyone.',
            upstreamPreview: textBody.slice(0, 500)
          });
        }

        // google.script.run compatibility:
        // Code.gs intentionally returns {"__error":"..."} when an action throws.
        // Convert that into a normal bridge failure instead of silently returning HTTP 200.
        if (data && data.__error) {
          console.error('SPES Apps Script action error:', action, data.__error);
          return res.status(500).json({
            success: false,
            __error: String(data.__error),
            message: String(data.__error)
          });
        }

        // Code.gs returns { success: true, result: ... }.
        // SPES.html expects the actual google.script.run function result.
        if (
          data &&
          data.success === true &&
          Object.prototype.hasOwnProperty.call(data, 'result')
        ) {
          return res.status(200).json(data.result);
        }

        if (data && data.success === false) {
          const message = String(
            data.message || data.error || 'SPES Apps Script action failed.'
          );
          return res.status(500).json({
            success: false,
            __error: message,
            message
          });
        }

        return res.status(200).json(data);
      } catch (err) {
        console.error('SPES bridge error:', err);
        return res.status(500).json({
          success: false,
          message: err && err.message ? err.message : 'SPES bridge failed.'
        });
      }
    });

   // ============================================================================
// SPES EMPLOYER DIAGNOSTIC
// Open: /api/spes/test
// Tests EXACTLY what getAllEmployers returns from Code.gs
// ============================================================================
app.get('/api/spes/test', async (req, res) => {
  try {
    const upstream = await fetch(SPES_APPS_SCRIPT_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain;charset=utf-8'
      },
      redirect: 'follow',
      body: JSON.stringify({
        action: 'getAllEmployers',
        args: [],
        context: {
          username: 'SERVER_TEST'
        }
      })
    });

    const textBody = await upstream.text();

    let parsed = null;

    try {
      parsed = JSON.parse(textBody);
    } catch (e) {
      return res.status(502).json({
        success: false,
        appsScriptUrl: SPES_APPS_SCRIPT_URL,
        upstreamStatus: upstream.status,
        message: 'Apps Script did not return valid JSON.',
        rawPreview: textBody.slice(0, 2000)
      });
    }

    // Code.gs normally returns:
    // {
    //   success: true,
    //   result: {
    //      success: true,
    //      records: [...]
    //   }
    // }

    let actualResult = parsed;

    if (
      parsed &&
      parsed.success === true &&
      Object.prototype.hasOwnProperty.call(parsed, 'result')
    ) {
      actualResult = parsed.result;
    }

    let records = [];

    if (Array.isArray(actualResult)) {
      records = actualResult;
    } else if (
      actualResult &&
      Array.isArray(actualResult.records)
    ) {
      records = actualResult.records;
    } else if (
      actualResult &&
      actualResult.result &&
      Array.isArray(actualResult.result.records)
    ) {
      records = actualResult.result.records;
    }

    // Only show LAGUNA first because that is what we are debugging.
    const lagunaRecords = records.filter(record => {
      return String(record.province || '')
        .trim()
        .toUpperCase() === 'LAGUNA';
    });

    // Make the output easier to inspect.
    const diagnosticRecords = lagunaRecords.map(record => ({
      sourceRow: record.sourceRow || '',

      province:
        record.province || '',

      employer:
        record.employer || '',

      incomeClassification:
        record.incomeClassification || '',

      cityMunicipality:
        record.cityMunicipality || '',

      wageRatePerDay:
        record.wageRatePerDay || '',

      headOfOffice:
        record.headOfOffice || '',

      position:
        record.position || '',

      addressee:
        record.addressee || '',

      employerShareInWageRate:
        record.employerShareInWageRate || '',

      percentageDoleCounterpart:
        record.percentageDoleCounterpart || ''
    }));

    return res.status(200).json({
      success: true,

      appsScriptUrl:
        SPES_APPS_SCRIPT_URL,

      upstreamStatus:
        upstream.status,

      totalEmployerRecords:
        records.length,

      totalLagunaEmployers:
        diagnosticRecords.length,

      lagunaEmployers:
        diagnosticRecords,

      rawAppsScriptResponse:
        parsed
    });

  } catch (err) {

    console.error(
      'SPES employer diagnostic error:',
      err
    );

    return res.status(500).json({
      success: false,
      appsScriptUrl:
        SPES_APPS_SCRIPT_URL,

      message:
        err && err.message
          ? err.message
          : String(err)
    });
  }
});

    // ============================================================================
    // NO-CACHE & SECURITY HEADERS
    // ============================================================================
    app.use((req, res, next) => {

      res.setHeader(
        'Cache-Control',
        'no-cache, no-store, must-revalidate, max-age=0, s-maxage=0, private'
      );

      res.setHeader(
        'Pragma',
        'no-cache'
      );

      res.setHeader(
        'Expires',
        '-1'
      );

      next();
    });


    // ============================================================================
    // PUBLIC DIRECTORY
    // ============================================================================
    // Your JSON/data folder is PUBLIC.
    const PUBLIC_DIR = path.join(__dirname, 'public');

    if (!fs.existsSync(PUBLIC_DIR)) {
      fs.mkdirSync(PUBLIC_DIR, {
        recursive: true
      });
    }

    const VIEWS_DIR = PUBLIC_DIR;


    // ============================================================================
    // BLOCK DIRECT DOWNLOAD OF SENSITIVE JSON FILES
    // ============================================================================
    // Since userEmails.json and otpStore.json are stored in public for persistence,
    // prevent browsers from downloading them directly.
    //
    // Other application JSON files remain accessible as before.
    // ============================================================================
    app.use((req, res, next) => {

      const blockedFiles = [
        '/userEmails.json',
        '/otpStore.json'
      ];

      const requestPath = String(req.path || '').toLowerCase();

      if (blockedFiles.includes(requestPath)) {
        return res.status(403).json({
          success: false,
          message: 'Direct access to this file is not allowed.'
        });
      }

      next();
    });


    // ============================================================================
    // STATIC PUBLIC FILES
    // ============================================================================
    // ============================================================================
// FAVICON - explicitly serve public/favicon.png for browser tab icons
// ============================================================================
app.get(['/favicon.png', '/favicon.ico'], (req, res) => {
  const faviconPath = path.join(PUBLIC_DIR, 'favicon.png');

  if (!fs.existsSync(faviconPath)) {
    console.error('Favicon not found:', faviconPath);
    return res.sendStatus(404);
  }

  res.setHeader('Content-Type', 'image/png');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  return res.sendFile(faviconPath);
});

// Performance: compress large, unchanged HTML pages without extra dependencies.
// Cached compressed bytes are refreshed when the source file changes.
const epmsZlib = require('zlib');
const epmsCompressedHtml = new Map();
app.use((req, res, next) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') return next();
  if (!/\bgzip\b/i.test(req.headers['accept-encoding'] || '')) return next();
  let pathname;
  try { pathname = decodeURIComponent(req.path); } catch (_) { return next(); }
  // Restrict to direct filenames under public, with no traversal or directories.
  if (!/^\/[a-zA-Z0-9_-]+\.html$/i.test(pathname)) return next();
  const filePath = path.join(PUBLIC_DIR, pathname.slice(1));
  let stat;
  try { stat = fs.statSync(filePath); if (!stat.isFile()) return next(); }
  catch (_) { return next(); }
  const key = filePath;
  let entry = epmsCompressedHtml.get(key);
  if (!entry || entry.mtimeMs !== stat.mtimeMs || entry.size !== stat.size) {
    try {
      entry = { mtimeMs: stat.mtimeMs, size: stat.size,
        data: epmsZlib.gzipSync(fs.readFileSync(filePath), { level: 6 }) };
      epmsCompressedHtml.set(key, entry);
    } catch (_) { return next(); }
  }
  res.setHeader('Content-Type', 'text/html; charset=UTF-8');
  res.setHeader('Content-Encoding', 'gzip');
  res.setHeader('Vary', 'Accept-Encoding');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Content-Length', entry.data.length);
  if (req.method === 'HEAD') return res.end();
  return res.end(entry.data);
});

// Static assets may be revalidated; HTML stays fresh after each deployment.
app.use(express.static(PUBLIC_DIR, {
  etag: true,
  lastModified: true,
  setHeaders(res, filePath) {
    if (/\.html$/i.test(filePath)) res.setHeader('Cache-Control', 'no-cache');
    else if (/\.(png|jpg|jpeg|webp|svg|ico|css|js)$/i.test(filePath))
      res.setHeader('Cache-Control', 'public, max-age=3600');
  }
}));

    // ============================================================================
    // REFERRAL & PLACEMENT EXCEL TEMPLATE
    // ============================================================================
    // Serve the XLSX through a dedicated endpoint instead of relying on the
    // browser to resolve a relative file URL. This is reliable even when the
    // Referral page itself is opened through a rewritten route.
    app.get('/api/referral-placement-template', (req, res) => {
      const templateName = 'Referral-and-Placement-Report-Form.xlsx';

      // Normal location: public/Referral-and-Placement-Report-Form.xlsx
      // Root fallback is kept only in case the repository currently has the
      // template beside server.js instead of inside public/.
      const candidates = [
        path.join(PUBLIC_DIR, templateName),
        path.join(__dirname, templateName)
      ];

      const templatePath = candidates.find(filePath => fs.existsSync(filePath));

      if (!templatePath) {
        console.error(
          'Referral Excel template not found. Checked:',
          candidates.join(' | ')
        );

        return res.status(404).json({
          success: false,
          message:
            'Referral-and-Placement-Report-Form.xlsx was not found on the deployed server.'
        });
      }

      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      );
      res.setHeader(
        'Content-Disposition',
        'inline; filename="' + templateName + '"'
      );

      return res.sendFile(templatePath);
    });



    // ============================================================================
    // PERSISTENT JSON DATA STORAGE UTILITIES
    // ============================================================================
    // IMPORTANT:
    // Your data folder is PUBLIC, therefore JSON files are stored directly in:
    //
    //     /public/
    //
    // Example:
    //
    //     public/jf_record.json
    //     public/venues.json
    //     public/schedules.json
    //     public/employers.json
    // ============================================================================
    const DATA_DIR = PUBLIC_DIR;

    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, {
        recursive: true
      });
    }


    function loadJsonData(filename, defaultValue) {

      const filePath = path.join(
        DATA_DIR,
        filename
      );

      if (fs.existsSync(filePath)) {

        try {

          return JSON.parse(
            fs.readFileSync(
              filePath,
              'utf8'
            )
          );

        } catch (e) {

          console.error(
            `Error reading ${filename}:`,
            e.message
          );

        }
      }

      return defaultValue;
    }


    function saveJsonData(filename, data) {

      const filePath = path.join(
        DATA_DIR,
        filename
      );

      try {

        fs.writeFileSync(
          filePath,
          JSON.stringify(
            data,
            null,
            2
          ),
          'utf8'
        );

      } catch (e) {

        console.error(
          `Error saving ${filename}:`,
          e.message
        );

      }
    }


    // ============================================================================
    // CENTRAL USER EMAIL DATABASE + OTP STORE
    // ============================================================================
    const AUTH_SHEET_API_URL =
      'https://script.google.com/macros/s/AKfycbx7nM-WiEmGx3duwdkOKTgmgPblXkQ72FjkCEKi9cA3nBM5tgRhKoL-ErqCPHOIgCPD/exec';

    async function callAuthSheetApi(payload) {
      const response = await fetch(AUTH_SHEET_API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload || {}),
        redirect: 'follow'
      });

      const text = await response.text();

      if (!response.ok) {
        throw new Error(`Authentication database HTTP ${response.status}: ${text}`);
      }

      try {
        return JSON.parse(text);
      } catch (e) {
        throw new Error('Authentication database returned invalid JSON: ' + text.slice(0, 300));
      }
    }

    // Compatibility helper for old/new Code.gs deployments.
    // Some deployments use getaccount/setemail/sendotp while others use
    // getUserEmail/setUserEmail/sendOtp (or similar names).  Instead of failing
    // immediately with "Unknown action.", try the compatible action aliases.
    async function callAuthSheetApiWithAliases(actions, payload) {
      const actionList = Array.isArray(actions) ? actions : [actions];
      let lastResult = null;
      let lastError = null;

      for (const action of actionList) {
        try {
          const result = await callAuthSheetApi({
            ...(payload || {}),
            action
          });

          lastResult = result;

          const message = String(
            result && result.message ? result.message : ''
          ).trim();

          const unknownAction =
            result &&
            result.success === false &&
            /unknown\s+(?:api\s+)?action|invalid\s+action|unsupported\s+action|action\s+not\s+found/i.test(message);

          if (!unknownAction) {
            return result;
          }
        } catch (error) {
          lastError = error;
        }
      }

      if (lastResult) return lastResult;
      if (lastError) throw lastError;

      return {
        success: false,
        message: 'Authentication database action failed.'
      };
    }

    async function getUserEmail(username) {
      const key = String(username || '').trim().toUpperCase();
      if (!key) return null;

      try {
        const result = await callAuthSheetApiWithAliases(
          ['getaccount'],
          { username: key }
        );

        return result && result.success === true && result.email
          ? String(result.email).trim().toLowerCase()
          : null;
      } catch (error) {
        console.error('Unable to get recovery email:', error.message);
        return null;
      }
    }

    async function setUserEmail(username, email) {
      const key = String(username || '').trim().toUpperCase();
      const cleanEmail = String(email || '').trim().toLowerCase();

      if (!key || !cleanEmail) {
        return {
          success: false,
          message: 'Username and email are required.'
        };
      }

      try {
        return await callAuthSheetApiWithAliases(
          ['setemail'],
          {
            username: key,
            email: cleanEmail
          }
        );
      } catch (error) {
        console.error('Unable to save recovery email:', error.message);
        return {
          success: false,
          message: 'Unable to save recovery email.'
        };
      }
    }

    let dbOtpStore = loadJsonData('otpStore.json', {});

    function saveOtpStore() {
      saveJsonData('otpStore.json', dbOtpStore);
    }

    function maskEmail(email) {

      if (
        !email ||
        !email.includes('@')
      ) {
        return email || '';
      }

      const [
        name,
        domain
      ] = email.split('@');

      if (name.length <= 2) {
        return (
          name[0] +
          '***@' +
          domain
        );
      }

      return (
        name.slice(0, 2) +
        '***@' +
        domain
      );
    }


    function generateOtp() {

      return String(
        Math.floor(
          100000 +
          Math.random() * 900000
        )
      );
    }


    function generateToken() {

      return crypto
        .randomBytes(24)
        .toString('hex');
    }


    function storeOtp(username, code) {

      const key = String(
        username || ''
      )
        .trim()
        .toUpperCase();

      const token =
        generateToken();

      const expiresAt =
        Date.now() +
        (15 * 60 * 1000);

      dbOtpStore[key] = {

        code: String(code),

        token,

        expiresAt,

        attempts: 0
      };

      saveOtpStore();

      return token;
    }


    function verifyOtp(username, code) {

      const key = String(
        username || ''
      )
        .trim()
        .toUpperCase();

      const entry =
        dbOtpStore[key];

      if (!entry) {

        return {
          success: false,
          message:
            'No OTP found. Please request a new one.'
        };
      }


      if (
        Date.now() >
        entry.expiresAt
      ) {

        delete dbOtpStore[key];

        saveOtpStore();

        return {
          success: false,
          message:
            'OTP has expired. Please request a new one.'
        };
      }


      entry.attempts =
        (entry.attempts || 0) + 1;


      if (entry.attempts > 5) {

        delete dbOtpStore[key];

        saveOtpStore();

        return {
          success: false,
          message:
            'Too many failed attempts. Please request a new OTP.'
        };
      }


      if (
        String(entry.code) !==
        String(code).trim()
      ) {

        saveOtpStore();

        return {
          success: false,
          message:
            'Invalid OTP. Please try again.'
        };
      }


      const token =
        entry.token;

      entry.verified = true;

      entry.code = null;

      saveOtpStore();

      return {
        success: true,
        token
      };
    }


    function consumeResetToken(
      username,
      token
    ) {

      const key = String(
        username || ''
      )
        .trim()
        .toUpperCase();

      const entry =
        dbOtpStore[key];

      if (
        !entry ||
        !entry.verified ||
        entry.token !== token
      ) {
        return false;
      }


      if (
        Date.now() >
        entry.expiresAt
      ) {

        delete dbOtpStore[key];

        saveOtpStore();

        return false;
      }


      delete dbOtpStore[key];

      saveOtpStore();

      return true;
    }


    // ============================================================================
    // SEND OTP EMAIL
    // ============================================================================
    async function sendOtpEmail(
      toEmail,
      otpCode,
      username
    ) {

      // OTP email delivery is handled by Google Apps Script / MailApp.
      // The Apps Script does NOT trust an arbitrary email address from Node.
      // It looks up the username in USER_ACCOUNTS and sends only to the
      // recovery email already saved for that account.
      const cleanUser = String(username || '').trim().toUpperCase();
      const cleanOtp = String(otpCode || '').trim();

      if (!cleanUser) {
        throw new Error('Username is required to send OTP.');
      }

      if (!/^\d{6}$/.test(cleanOtp)) {
        throw new Error('Invalid OTP code.');
      }

      const result = await callAuthSheetApiWithAliases(
        ['sendotp'],
        {
          username: cleanUser,
          otp: cleanOtp
        }
      );

      if (!result || result.success !== true) {
        throw new Error(
          result && result.message
            ? result.message
            : 'Google Apps Script failed to send the OTP email.'
        );
      }

      return {
        sent: true,
        simulated: false,
        email: result.email || maskEmail(toEmail)
      };
    }


    // ============================================================================
    // DATA STORE INITIALIZATIONS
    // ============================================================================
    let dbVenues =
      loadJsonData(
        'venues.json',
        [
          "BATANGAS CITY CONVENTION CENTER",
          "SM CITY DASMARINAS CAVITE",
          "CALAMBA CITY HALL LAGUNA",
          "LUCENA CITY CAPITOL QUEZON",
          "YNARES CENTER ANTIPOLO RIZAL"
        ]
      );


    let dbSchedules =
      loadJsonData(
        'schedules.json',
        []
      );


    let dbEmployers =
      loadJsonData(
        'employers.json',
        []
      );


    let dbApplicantPreReg =
      loadJsonData(
        'applicantPreReg.json',
        []
      );


    let dbTab3Applicants =
      loadJsonData(
        'tab3Applicants.json',
        []
      );


    let dbMERData =
      loadJsonData(
        'merData.json',
        []
      );


    let dbEmployerLockState =
      loadJsonData(
        'employerLockState.json',
        false
      );


    // ============================================================================
    // JOB FAIR DATABASE
    // ============================================================================
    // PRIMARY:
    //
    // public/jf_record.json
    //
    // LEGACY COMPATIBILITY:
    //
    // data/jf_records.json
    //
    // If the old file exists, it can still be read.
    // New records are saved to public/jf_record.json.
    // ============================================================================
    const JF_DB_PATH =
      path.join(
        PUBLIC_DIR,
        'jf_record.json'
      );


    const LEGACY_JF_DB_PATH =
      path.join(
        __dirname,
        'data',
        'jf_records.json'
      );


    function getJfRecords() {

      try {

        let filePath =
          JF_DB_PATH;


        // Use public/jf_record.json first
        if (!fs.existsSync(filePath)) {

          // Fall back to old data/jf_records.json
          if (
            fs.existsSync(
              LEGACY_JF_DB_PATH
            )
          ) {
            filePath =
              LEGACY_JF_DB_PATH;
          }
        }


        if (
          fs.existsSync(filePath)
        ) {

          const parsed =
            JSON.parse(
              fs.readFileSync(
                filePath,
                'utf8'
              )
            );


          if (!Array.isArray(parsed)) {
            return [];
          }


          return parsed.map(
            (r, i) => ({
              rowIndex:
                r.rowIndex ||
                (i + 2),

              ...r
            })
          );
        }

      } catch (err) {

        console.error(
          "Error reading JF database file:",
          err.message
        );

      }

      return [];
    }


    function saveJfRecords(records) {

      try {

        fs.writeFileSync(
          JF_DB_PATH,
          JSON.stringify(
            Array.isArray(records)
              ? records
              : [],
            null,
            2
          ),
          'utf8'
        );

      } catch (err) {

        console.error(
          "Error writing JF database file:",
          err.message
        );

      }
    }


    // ============================================================================
    // SPRS / CONSOLIDATED REPORT HELPERS
    // ============================================================================

    const SPRS_PROVINCES = [
      "BATANGAS",
      "CAVITE",
      "LAGUNA",
      "QUEZON",
      "RIZAL"
    ];


    function normalizeText(value) {

      return String(
        value == null
          ? ''
          : value
      )
        .trim();
    }


    function normalizeProvince(value) {

      const original =
        normalizeText(value);

      if (!original) {
        return '';
      }


      const upper =
        original
          .toUpperCase()
          .replace(/\s+/g, ' ')
          .trim();


      // Exact / common names
      if (
        upper === 'BATANGAS' ||
        upper.includes('BATANGAS')
      ) {
        return 'BATANGAS';
      }


      if (
        upper === 'CAVITE' ||
        upper.includes('CAVITE')
      ) {
        return 'CAVITE';
      }


      if (
        upper === 'LAGUNA' ||
        upper.includes('LAGUNA')
      ) {
        return 'LAGUNA';
      }


      if (
        upper === 'QUEZON' ||
        upper.includes('QUEZON')
      ) {
        return 'QUEZON';
      }


      if (
        upper === 'RIZAL' ||
        upper.includes('RIZAL')
      ) {
        return 'RIZAL';
      }


      // Common city/office indicators
      if (
        upper.includes('LUCENA')
      ) {
        return 'QUEZON';
      }


      if (
        upper.includes('ANTIPOLO')
      ) {
        return 'RIZAL';
      }


      if (
        upper.includes('DASMARINAS') ||
        upper.includes('IMUS') ||
        upper.includes('TAGAYTAY')
      ) {
        return 'CAVITE';
      }


      if (
        upper.includes('CALAMBA') ||
        upper.includes('SANTA ROSA') ||
        upper.includes('STA. ROSA') ||
        upper.includes('STA ROSA')
      ) {
        return 'LAGUNA';
      }


      if (
        upper.includes('BATANGAS CITY') ||
        upper.includes('LIPA') ||
        upper.includes('TANAUAN')
      ) {
        return 'BATANGAS';
      }


      return upper;
    }


    function firstValue(
      object,
      keys,
      defaultValue = ''
    ) {

      for (const key of keys) {

        if (
          object &&
          object[key] !== undefined &&
          object[key] !== null &&
          String(object[key]).trim() !== ''
        ) {
          return object[key];
        }
      }

      return defaultValue;
    }


    function numberValue(
      value,
      defaultValue = 0
    ) {

      if (
        value === null ||
        value === undefined ||
        value === ''
      ) {
        return defaultValue;
      }


      const cleaned =
        String(value)
          .replace(/,/g, '')
          .trim();


      const n =
        Number(cleaned);


      return Number.isFinite(n)
        ? n
        : defaultValue;
    }


    function metricValue(
      record,
      metricKeys,
      directKeys = []
    ) {

      const metrics =
        record &&
        typeof record.metrics === 'object'
          ? record.metrics
          : {};


      const nested =
        firstValue(
          metrics,
          metricKeys,
          null
        );


      if (
        nested !== null &&
        nested !== undefined &&
        nested !== ''
      ) {
        return numberValue(nested);
      }


      const direct =
        firstValue(
          record,
          directKeys.length
            ? directKeys
            : metricKeys,
          null
        );


      if (
        direct !== null &&
        direct !== undefined &&
        direct !== ''
      ) {
        return numberValue(direct);
      }


      return 0;
    }


    // ============================================================================
    // DATE PARSING FOR SPRS MONTH FILTER
    // ============================================================================
    function parseReportDate(value) {

      if (
        value === null ||
        value === undefined ||
        value === ''
      ) {
        return null;
      }


      if (value instanceof Date) {

        return isNaN(value.getTime())
          ? null
          : value;
      }


      const text =
        String(value).trim();


      if (!text) {
        return null;
      }


      // YYYY-MM-DD
      let match =
        text.match(
          /^(\d{4})-(\d{1,2})-(\d{1,2})$/
        );


      if (match) {

        const year =
          Number(match[1]);

        const month =
          Number(match[2]) - 1;

        const day =
          Number(match[3]);

        const d =
          new Date(
            year,
            month,
            day
          );

        return isNaN(d.getTime())
          ? null
          : d;
      }


      // MM/DD/YYYY
      match =
        text.match(
          /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/
        );


      if (match) {

        const month =
          Number(match[1]) - 1;

        const day =
          Number(match[2]);

        const year =
          Number(match[3]);

        const d =
          new Date(
            year,
            month,
            day
          );

        return isNaN(d.getTime())
          ? null
          : d;
      }


      // DD/MM/YYYY
      match =
        text.match(
          /^(\d{1,2})-(\d{1,2})-(\d{4})$/
        );


      if (match) {

        const day =
          Number(match[1]);

        const month =
          Number(match[2]) - 1;

        const year =
          Number(match[3]);

        const d =
          new Date(
            year,
            month,
            day
          );

        return isNaN(d.getTime())
          ? null
          : d;
      }


      const parsed =
        new Date(text);


      if (!isNaN(parsed.getTime())) {
        return parsed;
      }


      return null;
    }


    function getMonthNameFromDate(value) {

      const date =
        parseReportDate(value);


      if (!date) {
        return '';
      }


      return date.toLocaleString(
        'en-US',
        {
          month: 'long'
        }
      ).toUpperCase();
    }


    function normalizeMonth(value) {

      const text =
        normalizeText(value)
          .toUpperCase();


      if (!text) {
        return 'ALL';
      }


      if (
        text === 'ALL' ||
        text === 'ALL MONTHS'
      ) {
        return 'ALL';
      }


      const monthMap = {

        'JAN': 'JANUARY',
        'JANUARY': 'JANUARY',

        'FEB': 'FEBRUARY',
        'FEBRUARY': 'FEBRUARY',

        'MAR': 'MARCH',
        'MARCH': 'MARCH',

        'APR': 'APRIL',
        'APRIL': 'APRIL',

        'MAY': 'MAY',

        'JUN': 'JUNE',
        'JUNE': 'JUNE',

        'JUL': 'JULY',
        'JULY': 'JULY',

        'AUG': 'AUGUST',
        'AUGUST': 'AUGUST',

        'SEP': 'SEPTEMBER',
        'SEPT': 'SEPTEMBER',
        'SEPTEMBER': 'SEPTEMBER',

        'OCT': 'OCTOBER',
        'OCTOBER': 'OCTOBER',

        'NOV': 'NOVEMBER',
        'NOVEMBER': 'NOVEMBER',

        'DEC': 'DECEMBER',
        'DECEMBER': 'DECEMBER'
      };


      return monthMap[text] || text;
    }


    // ============================================================================
    // CONVERT ONE JF RECORD TO SPRS REPORT RECORD
    // ============================================================================
    function normalizeJfRecordForSPRS(record) {

      const metrics =
        record &&
        typeof record.metrics === 'object'
          ? record.metrics
          : {};


      const provinceSource =
        firstValue(
          record,
          [
            'province',
            'Province',
            'fieldOffice',
            'field_office',
            'office',
            'location'
          ],
          ''
        );


      const province =
        normalizeProvince(
          provinceSource
        );


      const date =
        firstValue(
          record,
          [
            'date',
            'dateOfJobFair',
            'jobFairDate',
            'job_fair_date',
            'date_of_job_fair'
          ],
          ''
        );


      const organizer =
        firstValue(
          record,
          [
            'organizer',
            'organizerName',
            'sponsor',
            'host',
            'employer'
          ],
          ''
        );


      const venue =
        firstValue(
          record,
          [
            'venue',
            'jobFairVenue',
            'job_fair_venue'
          ],
          ''
        );


      const lpoNo =
        firstValue(
          record,
          [
            'lpoNo',
            'lpoNumber',
            'lpo_number',
            'documentNumber',
            'documentNo',
            'document_number'
          ],
          ''
        );


      const dateReceived =
        firstValue(
          record,
          [
            'dateReceived',
            'date_received',
            'dateFiled',
            'date_filed'
          ],
          ''
        );


      const dateIssued =
        firstValue(
          record,
          [
            'dateIssued',
            'date_issued'
          ],
          ''
        );


      // --------------------------------------------------------------------------
      // METRICS
      // --------------------------------------------------------------------------

      const comp_tot =
        metricValue(
          record,
          [
            'comp_tot',
            'companies',
            'companyTotal',
            'totalCompanies',
            'participatingCompanies'
          ]
        );


      const comp_loc =
        metricValue(
          record,
          [
            'comp_loc',
            'localCompanies',
            'companiesLocal',
            'local_companies'
          ]
        );


      const comp_ovs =
        metricValue(
          record,
          [
            'comp_ovs',
            'overseasCompanies',
            'companiesOverseas',
            'overseas_companies'
          ]
        );


      const vac_tot =
        metricValue(
          record,
          [
            'vac_tot',
            'vacancies',
            'vacancyTotal',
            'totalVacancies'
          ]
        );


      const vac_loc =
        metricValue(
          record,
          [
            'vac_loc',
            'localVacancies',
            'vacanciesLocal',
            'local_vacancies'
          ]
        );


      const vac_ovs =
        metricValue(
          record,
          [
            'vac_ovs',
            'overseasVacancies',
            'vacanciesOverseas',
            'overseas_vacancies'
          ]
        );


      const app_m =
        metricValue(
          record,
          [
            'app_m',
            'appMale',
            'applicantsMale',
            'maleApplicants'
          ]
        );


      const app_f =
        metricValue(
          record,
          [
            'app_f',
            'appFemale',
            'applicantsFemale',
            'femaleApplicants'
          ]
        );


      let app_tot =
        metricValue(
          record,
          [
            'app_tot',
            'applicants',
            'registeredApplicants',
            'totalApplicants'
          ]
        );


      if (
        app_tot === 0 &&
        (app_m > 0 || app_f > 0)
      ) {
        app_tot =
          app_m + app_f;
      }


      const hots_m =
        metricValue(
          record,
          [
            'hots_m',
            'hiredOnTheSpotMale',
            'hiredMale'
          ]
        );


      const hots_f =
        metricValue(
          record,
          [
            'hots_f',
            'hiredOnTheSpotFemale',
            'hiredFemale'
          ]
        );


      let hots_tot =
        metricValue(
          record,
          [
            'hots_tot',
            'hots',
            'hiredOnTheSpot',
            'hired_on_the_spot'
          ]
        );


      if (
        hots_tot === 0 &&
        (hots_m > 0 || hots_f > 0)
      ) {
        hots_tot =
          hots_m + hots_f;
      }


      const qual_m =
        metricValue(
          record,
          [
            'qual_m',
            'qualifiedMale'
          ]
        );


      const qual_f =
        metricValue(
          record,
          [
            'qual_f',
            'qualifiedFemale'
          ]
        );


      let qual_tot =
        metricValue(
          record,
          [
            'qual_tot',
            'qualified',
            'qualifiedApplicants'
          ]
        );


      if (
        qual_tot === 0 &&
        (qual_m > 0 || qual_f > 0)
      ) {
        qual_tot =
          qual_m + qual_f;
      }


      const near_m =
        metricValue(
          record,
          [
            'near_m',
            'nearHireMale',
            'nearHiredMale'
          ]
        );


      const near_f =
        metricValue(
          record,
          [
            'near_f',
            'nearHireFemale',
            'nearHiredFemale'
          ]
        );


      let near_tot =
        metricValue(
          record,
          [
            'near_tot',
            'nearHire',
            'nearHired',
            'near_hire'
          ]
        );


      if (
        near_tot === 0 &&
        (near_m > 0 || near_f > 0)
      ) {
        near_tot =
          near_m + near_f;
      }


      const unqual_m =
        metricValue(
          record,
          [
            'unqual_m',
            'unqualifiedMale'
          ]
        );


      const unqual_f =
        metricValue(
          record,
          [
            'unqual_f',
            'unqualifiedFemale'
          ]
        );


      let unqual_tot =
        metricValue(
          record,
          [
            'unqual_tot',
            'unqualified',
            'unqualifiedApplicants'
          ]
        );


      if (
        unqual_tot === 0 &&
        (unqual_m > 0 || unqual_f > 0)
      ) {
        unqual_tot =
          unqual_m + unqual_f;
      }


      const placed_m =
        metricValue(
          record,
          [
            'placed_m',
            'placedMale',
            'placementMale'
          ]
        );


      const placed_f =
        metricValue(
          record,
          [
            'placed_f',
            'placedFemale',
            'placementFemale'
          ]
        );


      let placed_30 =
        metricValue(
          record,
          [
            'placed_30',
            'placed30',
            'placements30',
            'placement30Days',
            'placementsWithin30Days',
            'placements'
          ]
        );


      if (
        placed_30 === 0 &&
        (placed_m > 0 || placed_f > 0)
      ) {
        placed_30 =
          placed_m + placed_f;
      }


      return {

        id:
          record.id ||
          `jf-${record.rowIndex || crypto.randomUUID()}`,

        rowIndex:
          record.rowIndex || '',

        province,

        fieldOffice:
          firstValue(
            record,
            [
              'fieldOffice',
              'field_office'
            ],
            ''
          ),

        year:
          firstValue(
            record,
            ['year'],
            ''
          ),

        reportingPeriod:
          firstValue(
            record,
            [
              'reportingPeriod',
              'month'
            ],
            ''
          ),

        lpoNo,

        dateReceived,

        dateIssued,

        organizer,

        date,

        venue,

        documentApplied:
          firstValue(
            record,
            [
              'documentApplied',
              'documentType'
            ],
            ''
          ),

        actionTaken:
          firstValue(
            record,
            [
              'actionTaken',
              'status'
            ],
            ''
          ),

        metrics: {

          comp_tot,
          comp_loc,
          comp_ovs,

          vac_tot,
          vac_loc,
          vac_ovs,

          app_tot,
          app_m,
          app_f,

          hots_tot,
          hots_m,
          hots_f,

          qual_tot,
          qual_m,
          qual_f,

          near_tot,
          near_m,
          near_f,

          unqual_tot,
          unqual_m,
          unqual_f,

          placed_30,
          placed_m,
          placed_f
        },

        // Keep original record available for compatibility
        originalRecord: record
      };
    }


    // ============================================================================
    // GET AVAILABLE PROVINCES FOR SPRS
    // ============================================================================
    app.get('/api/provinces', (req, res) => {

      try {

        const records =
          getJfRecords();


        const provinces =
          new Set(
            SPRS_PROVINCES
          );


        records.forEach(
          record => {

            const province =
              normalizeProvince(
                firstValue(
                  record,
                  [
                    'province',
                    'fieldOffice',
                    'field_office',
                    'office',
                    'location'
                  ],
                  ''
                )
              );


            if (province) {
              provinces.add(
                province
              );
            }

          }
        );


        const ordered =
          SPRS_PROVINCES
            .filter(p =>
              provinces.has(p)
            );


        for (
          const province of provinces
        ) {

          if (
            !ordered.includes(
              province
            )
          ) {
            ordered.push(
              province
            );
          }
        }


        return res.json({
          success: true,
          provinces: ordered
        });

      } catch (err) {

        console.error(
          'Error loading provinces:',
          err
        );

        return res.status(500).json({
          success: false,
          message:
            err.message ||
            'Failed to load provinces.',
          provinces:
            SPRS_PROVINCES
        });

      }
    });


    // ============================================================================
    // SPRS MUNICIPALITIES
    // ============================================================================
    // SPRS.html loads the municipality list after a province is selected.
    // The list is kept here so the page works even when the JF database has
    // no records yet.
    // ============================================================================
    const SPRS_MUNICIPALITIES = {
      BATANGAS: [
        'Agoncillo','Alitagtag','Balayan','Batangas City','Bauan','Calaca',
        'Calatagan','Cuenca','Ibaan','Laurel','Lemery','Lian','Lipa City',
        'Lobo','Mabini','Malvar','Mataasnakahoy','Nasugbu','Padre Garcia',
        'Rosario','San Jose','San Juan','San Luis','San Nicolas','San Pascual',
        'Santo Tomas','Taal','Tanauan City','Tingloy','Tuy'
      ],
      CAVITE: [
        'Alfonso','Amadeo','Bacoor City','Carmona','Cavite City','Dasmariñas City',
        'General Emilio Aguinaldo','General Mariano Alvarez','General Trias City',
        'Imus City','Indang','Kawit','Magallanes','Maragondon','Mendez','Naic',
        'Noveleta','Rosario','Silang','Tagaytay City','Tanza','Ternate',
        'Trece Martires City'
      ],
      LAGUNA: [
        'Alaminos','Bay','Biñan City','Cabuyao City','Calamba City','Calauan',
        'Cavinti','Famy','Kalayaan','Liliw','Los Baños','Luisiana','Lumban',
        'Mabitac','Magdalena','Majayjay','Nagcarlan','Paete','Pagsanjan','Pakil',
        'Pangil','Pila','Rizal','San Pablo City','San Pedro City','Santa Cruz',
        'Santa Maria','Santa Rosa City','Siniloan','Victoria'
      ],
      QUEZON: [
        'Agdangan','Alabat','Atimonan','Buenavista','Burdeos','Calauag',
        'Candelaria','Catanauan','Dolores','General Luna','General Nakar',
        'Guinayangan','Gumaca','Infanta','Jomalig','Lopez','Lucban','Lucena City',
        'Macalelon','Mauban','Mulanay','Padre Burgos','Pagbilao','Panukulan',
        'Patnanungan','Perez','Pitogo','Plaridel','Polillo','Quezon','Real',
        'Sampaloc','San Andres','San Antonio','San Francisco','San Narciso',
        'Sariaya','Tagkawayan','Tayabas City','Tiaong','Unisan'
      ],
      RIZAL: [
        'Angono','Antipolo City','Baras','Binangonan','Cainta','Cardona','Jalajala',
        'Morong','Pililla','Rodriguez','San Mateo','Tanay','Taytay','Teresa'
      ]
    };

    app.get(
      '/api/municipalities',
      (req, res) => {

        try {

          const province =
            normalizeProvince(
              req.query.province || ''
            );

          const known =
            SPRS_MUNICIPALITIES[province] || [];

          // Also include municipalities already present in the JF records.
          // This allows encoded data to appear even if a municipality was not
          // included in the fixed CALABARZON list above.
          const found = new Set(known);

          getJfRecords().forEach(record => {
            const recordProvince = normalizeProvince(
              firstValue(
                record,
                [
                  'province','Province','fieldOffice','field_office',
                  'office','location'
                ],
                ''
              )
            );

            if (recordProvince !== province) return;

            const municipality = normalizeText(
              firstValue(
                record,
                [
                  'municipality','Municipality','municipalityCity',
                  'municipality_city','city','cityMunicipality',
                  'city_municipality'
                ],
                ''
              )
            );

            if (municipality) {
              found.add(municipality);
            }
          });

          return res.json({
            success: true,
            province,
            municipalities: Array.from(found).sort(
              (a, b) => a.localeCompare(b)
            )
          });

        } catch (err) {

          console.error(
            'Error loading municipalities:',
            err
          );

          return res.status(500).json({
            success: false,
            message:
              err.message ||
              'Failed to load municipalities.',
            municipalities: []
          });
        }
      }
    );


    // ============================================================================
    // RA 11261 ROSTER: ISOLATED GOOGLE SHEETS READER (ROW 11, COLUMNS A:P)
    // This route does not change existing SPRS, GIP, SPES, or other APIs.
    // GET /api/ra11261/roster?spreadsheetId=ID&sheet=SHEET_NAME
    // If sheet is omitted, use the confirmed RA 11261 worksheet: Encoding Form.
    // ============================================================================
    app.get('/api/ra11261/roster', async (req, res) => {
      try {
        let id = String(req.query.spreadsheetId || req.query.id || '').trim();
        const match = id.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
        if (match) id = match[1];
        if (!/^[a-zA-Z0-9_-]{20,}$/.test(id)) {
          return res.status(400).json({success:false, message:'A valid Google Sheets spreadsheetId is required.',records:[],data:[]});
        }
        const sheet = String(req.query.sheet || req.query.sheetName || 'Encoding Form').trim();
        if (sheet.length > 150) return res.status(400).json({success:false,message:'Invalid worksheet name.',records:[],data:[]});
        const params = new URLSearchParams({tqx:'out:json',headers:'0',range:'A11:P'});
        if (sheet) params.set('sheet',sheet);
        const url = 'https://docs.google.com/spreadsheets/d/' + encodeURIComponent(id) + '/gviz/tq?' + params.toString();
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 20000);
        let response, body;
        try {
          response = await fetch(url,{redirect:'follow',signal:controller.signal});
          body = await response.text();
        } finally {clearTimeout(timer);}
        if (!response.ok || /^\s*</.test(body)) {
          return res.status(502).json({success:false,message:'Google Sheets could not be read. Verify sharing permissions and the worksheet name.',upstreamStatus:response.status,records:[],data:[]});
        }
        const start = body.indexOf('{'), end = body.lastIndexOf('}');
        if (start < 0 || end <= start) throw new Error('Google Sheets returned an invalid response.');
        const payload = JSON.parse(body.slice(start,end+1));
        if (payload.status === 'error') {
          const details = (payload.errors || []).map(e=>e.detailed_message || e.message).filter(Boolean).join('; ');
          return res.status(502).json({success:false,message:details || 'Google Sheets query failed.',records:[],data:[]});
        }
        const rawRows = (payload.table && payload.table.rows) || [];
        const cols = ['reportingMonth','barangay','certificateNumber','lastName','firstName','middleName','suffix','age','birthMonth','birthDay','birthYear','male','female','maritalStatus','educationalAttainment','pwd'];
        const records = rawRows.map((r,index)=>{
          const cells = (r && r.c) || [];
          const values = cols.map((_,i)=>{
            const c = cells[i];
            return c == null ? '' : String(c.f != null ? c.f : (c.v != null ? c.v : '')).trim();
          });
          // Sex and PWD markers: keep blank cells blank, normalize selected cells to X.
          const selectedMarker = value => /^(x|yes|y|true|1|checked|✓|✔)$/i.test(String(value).trim()) ? 'X' : '';
          values[11] = selectedMarker(values[11]);
          values[12] = selectedMarker(values[12]);
          values[15] = selectedMarker(values[15]);
          const record = {sourceRow:index+11};
          cols.forEach((key,i)=>{record[key]=values[i];});
          record.fullName = [record.firstName,record.middleName,record.lastName].filter(Boolean).join(' ');
          record.__values = values;
          return record;
        }).filter(r=>r.lastName || r.firstName);
        return res.json({success:true,spreadsheetId:id,sheet:sheet || '(default worksheet)',startRow:11,count:records.length,records,data:records});
      } catch (err) {
        console.error('RA 11261 roster fetch error:',err);
        return res.status(500).json({success:false,message:err.name === 'AbortError' ? 'Google Sheets request timed out.' : err.message,records:[],data:[]});
      }
    });

    // ============================================================================
    // SPRS DIRECT GOOGLE SHEETS SOURCE
    // Reads the Province -> Municipality spreadsheet mapping used by SPRS.
    // This is intentionally server-side so the browser does not depend on
    // cross-origin Google Sheets requests.
    // ============================================================================
    const SPRS_GOOGLE_SHEET_DATABASE = {"BATANGAS":{"AGONCILLO":"1GaEHV-EzfOo9o8DeTdr03r-YcnHHtfltui_mAVahoXg","ALITAGTAG":"1o-f_R-TJGNHBMMuUa7TWQHKe3A6YAyKKcoHbJX5NVQI","BALAYAN":"1eBlB2ExkUTr3jKkDsZkkAfLxMc_MeuLRAZA1AViIWiY","BALETE":"1lrDw3B4gv7pLypT-DbYpGXw2HvBjNl5Haelhph410jU","BATANGAS CITY":"1JLffWeA0yLbvVsrcL5Wg0FdT8tCfDQvDvkTILx6CZdM","BATANGAS PROVINCE":"1tVQd-80rFlHlPS_PXRGagLbpEBf_oak7v4jBZiWUmcU","BATANGAS STATE UNIVERSITY":"1i6t80eN21H7mS3teuH70sgORuZCZ41sg-tR7XSuQky","BAUAN":"1uQZYeiynteUPeazWhxQgtF_23-kVgaB3H-2y9qcki88","CALACA":"1yEwSFFCcnOyIXkvIN54mIZFephOSOn75XXRV_i4L5tY","CALATAGAN":"1ojIRL1xFNFywH-2r6n6k5SsRtUBGhnn-S4HKd1rsgo","CUENCA":"170jmFDAcsAP8bZAlgWJDcS6hcOwf14whp9lKgzwkHmk","IBAAN":"1tebXLyOlWy8VnTv3vfpMmGfbZuDEnYYKPF6Tf1iMAlo","LAUREL":"1-hMVdEIC49f9mKkQLW6cs3mW9vI90qMK5LJCBwDdqmA","LEMERY":"1fqaICqUf4zL6-mZ8oH2rmYUzLZz6rUaRF7TdsstBQrQ","LIAN":"1NAFH8gGqgt0IkVNNGLb96wQ0A8sHTyC3-f0iyaqV678","LIPA CITY":"1SW3XQD9p-KUz4h_jKhoQzeBSmcowkqcqzqYG3AIvNcc","LOBO":"1lVBfKt-1vtKSvrFc5z_P26wc-QQIT-eHkGwrM_pIMj4","LYCEUM OF THE PHILIPPINES UNIVERSITY-BATANGAS":"14awT3KTBW6F2zoQzTe7E6jfBNK0EUH0ARocrBYxJ8Wc","MABINI":"1vOsa5XMiJlOi6Fv4OUqbpzlgDpogSErQ8o7qeNBzF_4","MALVAR":"1LdzYePhsZ_JjoDgoill4ttTGr9pRHr_w6qDjy62ytI4","MATAASNAKAHOY":"12aXHYc6Re_cQcIY_T1TxztEcgrmR_NGG1v6_-rmK1yU","NASUGBU":"1YRheGeFw39ga_NSEg2x78ueNoIT5tIhrEZ6U7BjR0KY","PADRE GARCIA":"1UHtvKn5npUItxkvDWMd2YhCAGq46R6TyfXN_Shno79Y","RIZAL COLLEGE OF TAAL":"1YarYEDDENllLnpy4sbTLyD4t80QTA9cyb4hsrsiq9qk","ROSARIO":"17DdxHsek3NLPVGBH0Pvw-maWTXtNYbs3uf5cb7a2W2g","SAN JOSE":"1KU8jar5y6RVifQfntbjz6HbDwJjqh2l5ugAwDI1sbo0","SAN JUAN":"10iiSJdJmcMjmjWO7TZTEcMvryMhWkiQSjby046LSKsU","SAN LUIS":"1qkB_F6tWKKsHLDiSwZgK5OeAnpTLF-AvK5j9-zCZNOU","SAN NICOLAS":"10kF3QxaNkqt_C5pT0MexQtXG9n4m9Pm8oHwdL22TC_0","SAN PASCUAL":"1vfxjllf3NdhxQwxZ_sCEeO3-IlKW9N3h_E8pyiQ_Ytw","SANTA TERESITA":"1z3Xl2Szj3d1pTeR4nOrsEy8-oF8eAaiWPVEQCPvSvZQ","SANTO TOMAS":"1UKDLR5pMf6SrT5rVymE45EopykaaqidcBdNi0iyAU-w","TAAL":"1RvsIg5xa0jVU0BLjBAtwtPuG1j6FwY9glV_rE2ofaI0","TALISAY":"1jzEnWeWb17UBG3WnytmT8tI-5cmTYA4B5U-D_3UNMDY","TANAUAN CITY":"1_XYu9hlV-lOvlbg-NtKvdlMPiucAPtkYpDrRGk7udUc","TAYSAN":"1pPB8BId0xqm4yCuiJJmkJIhAcPAdzEwmqZ72dHjQ_0w","TINGLOY":"1hcpPMfQWKRUNiWi9iox6a5w1pLfg5TJ8k1uyWnrOJe0","TUY":"1xcbj5LoOGUA24anbzz-ltqvnkQEtpZRhmTG0eDP3l_4","UNIVERSITY OF BATANGAS-MAIN":"1XT_PmlMkt0iNSV6CTwctRFhitWdp3wvSGku_6k7reFg","NATIONAL UNIVERSITY-LIPA":"1aYQ6GxgGbBohkdmOCl38PscCUv4ggcjOiUGvd8FwT4A"},"CAVITE":{"ALFONSO":"1ABrk3NbHz0Us3MWwgEsZE-X_dQ_fCKBqiJyxbRH2wpU","AMADEO":"1fWlXUm1oaUrbL-S0iQCTErzqtNHJyBE2FedjUsRoJ5Q","BACOOR CITY":"1jDFNrWOyQEUO_wkQgNlETErtlJ6-e8TKM2957BQYjjk","CARMONA":"1QHVxPwu3lIR0iNYZ0SkoY4agAXhLIEQBmcO6jsnDVHk","CAVITE CITY":"1nvGiPZB8yE-XqTmFZGXHwdMFAYTw1KHtysss2gZZLtM","DASMARIÑAS CITY":"1jL_aXP7ZqBks4JXH4xhG6edcnT1r1gQNxZ0elQMXfwk","GENERAL EMILIO AGUINALDO":"15bsGY6NOtsofdqNbQBa1HlFSjVWy6PQvIlhEcYa8GOk","GENERAL MARIANO ALVAREZ":"1SIjfUFQRuQYy5XP9Ly0DZ7bmoa-NUkXeHW2CG-3h094","CITY OF GENERAL TRIAS":"1_WDh9oG8YfNVDkYP1JGkacH5xrnu2yHzunjQL-2dyuI","IMUS CITY":"1wlQm_gcVxF_p38rUuQKeZQ-NX3ap_Xo8LkfxeontMoo","INDANG":"1IUdhmXeHuWJLzUejjANiHrmMAYfOFn2OmFftK5gzsho","KAWIT":"1AAZihURPbWTd3gn-pk5XFy5e8RFSoSuFy5KJc52xK3M","MAGALLANES":"1vQvBA67qGNbJeglUZRb3ufEkSjE7weAlIKVmz_22qys","MARAGONDON":"1kgajvJlPC3IhMeWPxoLApOcEz1gN8hpTK4U_2ZA0-Ic","MENDEZ":"1XIZ-bDKnZDSJS3rJmo6CBsTmxBnPSx_jrWKAwhFskQc","NAIC":"1aK--jE27lMukIDsehVOlDnRtWZfooyNq_Gcb6Ul8G2w","NOVELETA":"14QPkD-5ecCOcYqHF0ohaP5p1HPGelmJM5EIuz72juCs","ROSARIO":"11gHb4P2GxQb8T0cbplfALaR8CtIR9O9xXe8fc-vI5Tk","SILANG":"1_WnzewG-YEOvED0VulrNeJ6E2gUrPFjZutKi1mGUhi4","TAGAYTAY CITY":"1z5aHG5JbSC0_eErCApOVyYy6cNuzBfoS_UN4g82ruLE","TANZA":"1ywKQJvxKRbQuefwE7cRJC85Pq_mON8ix9CG89U-WXwE","TERNATE":"19StLjKTu1vXbXH7ECd7cuGxIR5CUwNvcd6Bg4g6Jy4w","TRECE MARTIRES CITY":"1LpvySoO8qJd60u4EgNVezNNlooB6IwsKcFmJtVzSKoA","CAVITE PROVINCE":"1dZMzczxDgkGotWXaj-7jGoa35L9Fx44CR7osidqZ7jE","DE LASALLE UNIVERSITY - CAVITE":"1uhBCWWnGAjGb10mXYaFZ_mPD0R-g3KXCUR8tGNDUsc8","LAGUNA POLYTECHNIC UNIVERSITY - CAVITE":"1WTNJ9gNtfDRLSvvl037hT_JI_LIHB2P2q-trw3u-rzg","NATIONAL COLLEGE OF SCIENCE & TECHNOLOGY - CAVITE":"1NhcZ_YEKMMYW-3wy5dzGdgzTHZFty-_xof8rpvpAKKA","NATIONAL UNIVERSITY - CAVITE":"1NbNhwO8uZuAH39vbkK-j4G161SmUvGLYfnYSn0-A-2w"},"LAGUNA":{"ALAMINOS":"1RgVb_KS2w0L32a3vU1FVO-XNIbSPxqf8nuX_cFfYBro","BAY":"1Nybe8I9eaSrKpiAiDX7Oeiew5Fg-gdofuZpckWlLG0M","CITY OF BIÑAN":"19hTNev7NjTgxE7hcRQ_Y9ZhFb0GKOP4OxV9KpRRSI8o","CABUYAO CITY":"1d_4EPZ2ZbaDxtapFggydToqAPmz_G9cjM29Slj-Gd8E","CITY OF CALAMBA":"1eCKXG7o6y3CjxSbKFWbUbQg7K32IiLMikRNI2p054cM","CALAUAN":"1FhCERhR8P-fDJFNV6AfFTmJa_1nbUAP_GY8zh3XDgsM","CAVINTI":"1G3V9OjKpIDpkLo3UbdkiCQXRbEftY0-Fxp6bJ4BWHo8","FAMY":"17xfCbaHFkcVi3ncrmdB5LOuWZP_MVAYPoeq4RUP_YqE","KALAYAAN":"1TORbSeUETOhdy-jFKR7tRhb412lSE3ftDSbyAsUFDhw","LILIW":"1QQcUUHL4AKm5tlPeVqNZBgTrQk-MJV-CDNnBOwDFKK8","LOS BAÑOS":"1DP8ONSdIs-A_EbnntN5Ezu2Os5QNTlDalkvhHSOGZQM","LUISIANA":"1Tjf8gn8mIDlYg56NUJ-AXMb8xtaLBzvv3RiR1kNyFC8","LUMBAN":"1R9Z8SJRs1JrqTihthjvZHvU_SM3xKfSdOvVNxt08ffw","MABITAC":"1pwg1qlCUH20HRABVpUp6rpZZYOjFY1mmSXQpHShBi_c","MAGDALENA":"1H2wlnte3WLO28IG5sKPYyAAzLbicXiZ55BYBXFrFdj8","MAJAYJAY":"1EdJAkVlPfc_PvmHCh76hGxnmY3Tm6-7Mfz2rMR3Egfk","NAGCARLAN":"1mqiSBxNIsHgC5rb0YAdNZ88vPgUwKv9zytODwfdXH6Q","PAETE":"11jMqQPh54E9tMNMTMP47IV8C388vR6rc-HOLEPXVaks","PAGSANJAN":"18cfyaHEUK76QL3pgYxJYN2soPxefZXk74P1uJt86z-o","PAKIL":"18sLNv1h0rl-w57X6AfFpO0bu7H1ezJIlHG6Hv9B95Ng","PANGIL":"1Ne6Nesd_KWNJM6oIEG5_VkfmJm868qNFRgWfITouaog","PILA":"1epn3fzDoEqao5BT8mKLLi559-kG1fV-vobTRpw05NLM","RIZAL":"1wEqW85p27P0kQivSpIEUz1Im04Vj44xPRBkydGG-UIM","SAN PABLO CITY":"1E2dOoxJ68GHLHfZq8AcS8klTBnOsqBDVGZ9UL_mpwos","SAN PEDRO CITY":"1ZhHc7Yajs6AsawG0UtS3tQdFqQ0w1oHZDp13MQkYGaA","SANTA CRUZ":"1gbxXfxOJWGB78AjqvtMsLXj0eDjA9rJ-G7zAQ8TzWkU","SANTA MARIA":"1J2ZZnp8emNEJvacxslQwv5UtJuRbLp_Ef3k6OBdM70M","CITY OF SANTA ROSA":"1KK-CE-Ur0DAYYKHQVr5DDzqEoEl4rPpa0xd0hUlt6WY","SINILOAN":"1KZ5vh_wJVuQaurNfsHjb4wgi5Mmla8F1fqFfyJuU6AE","VICTORIA":"1L4_aTJ21B3ZPZmk9JUq4zSzsmrp05WMDuQ5siXu8E6w","LAGUNA PROVINCE":"1ZsnVowaC6Q0PVriMNtFjp1ilVpH3-HU4w5pceB_A7fY","LAGUNA STATE POLYTECHNIC UNIVERSITY-MAIN":"1M-P6rq_LePaLj_igKxtfixhqY5jh3cfghya6nTk17so","LAGUNA UNIVERSITY":"1NBDLTWJj6TFu68q4hvtZBFOhQZb2uMVO-abCJYGJisI","RIZAL COLLEGE OF LAGUNA":"1sc8khc9OCdPHWOADxehaLg-tQSN7pxmUwgaAtrQsXaA","TRIMEX":"1dgF2YdfY9F8FVdW85TemwdFSVOIFyh2oX6f3p5PUEZ4","UCL":"1zBW48zO4_itjwBySvQvTtNMs0V5PN9PgzXOYhy_p4I4","PHINMA RIZAL":"19DRkM1-dyhxx3eO1RreUcwY8JY6uiDz743u49Ia3ZZY","PHINMA Union College of Laguna":"1PKgIs1mPVRSVQ6e2A-8ygR-Cyuc8gUQnusgT3eU5E00"},"QUEZON":{"AGDANGAN":"1feEKJmuHZ5m6HHZ1YojN1WDePQlqMaPeo5zKHdF5lh8","ALABAT":"1pRIzdc5JYQbBgFyrdhVr_dHJE3TMmQqfh932d51wyxs","ATIMONAN":"1L6nUt_10UAo6jNVz0TN7jbpb3dZIZf6CzzLZ-wexltk","BUENAVISTA":"1AjPsURnGO_5r9JSKX8w0_TcyelKboCZ-bL9cBYG-giI","BURDEOS":"1dvEds0XFhuKMi2odQEmDAkBjRKd6khtTUF62LgsykDI","CALAUAG":"1adKlvdshLDDtpexZE8bwi14v4h3e3VZ2kDtMCzrIgzU","CANDELARIA":"1AU-P3KXxKfpBLnsVdCZ5JNFeE1eytshdggkyMglEKF4","CATANAUAN":"1ZfD89VVf3fuSp6F5oMsoSvgTEPylhfnP_3ntrm0c3sE","DOLORES":"1yprluLceC5nuQOtwBgKx17G241UMYHWCnMIuajPC8I8","GENERAL LUNA":"1jX0wiiVK3DzxT6x2ogY0NQIvmwJsMsyJ8QZnRMAHrxk","GENERAL NAKAR":"1M5eIOgZ0ZOfNBLADrdbuR1MCD70_kjNxAICCZrwnbig","GUINAYANGAN":"1HB4oj__Z0DD5Plv46Ue4uSNkL-gMZkmkPk0ZXnGlXhE","GUMACA":"1wTQF-yQvBzsAqq8fM2_F8bOGRj2RMouLqOCTBNc6tJk","INFANTA":"1-1Wgg6C6-uY0QxDAJfg8g1BDg7Z6gjPHJaxCxq9b9-8","JOMALIG":"1xeysYsf_G9LHRfarjC_VeyCppw_kexQciAYTCc3ehJg","LOPEZ":"1nFH9vKjSmMA95vZj7XqMB1YeJeRqVjm8ytDTfr0gf80","LUCBAN":"10A_Nl-TqYxuCMBkrEtUq-gC5SGTIO8wutEqaCUyynxo","LUCENA CITY":"11YILMU1UTFdcXN-8Hgs2PBSOqwZvu5pru5L6Mui7qy0","MACALELON":"1Cux63-SftULX5lD3l3ext54UcVOF6G7_xkeHyqLS0b8","MAUBAN":"158_Kd4yhAmaYzxwCHcywY6Er_ogBzQLtKqpbk-etNhY","MULANAY":"1r0LGlLqwJvFL3fb2kT4vPzcRCOLe23wgsPLoifMjLX8","PADRE BURGOS":"1JWhNuCpO1vikCVfnd9As_6-gG80QxUFfE-Mz8z921Mg","PAGBILAO":"1CBEu8pfziS3N_Vn7CdSGaSP7bCgKI58CDsY5ajfy-Ts","PANUKULAN":"1Rq_exAi-VzC4LPa9DhlHf-lXekqGCB_WyI_IDmORMNE","PATNANUNGAN":"18825UE-wvUeZ-H_dkOAA44yykjppoaIrx3C0CeCGgVs","PEREZ":"1blMsZ0WoZn6uN0T48pnVC7pEk3ol8SNQapioSEV8vkc","PITOGO":"1A8_Pgpx56X_YD5IYLCVkjl-jHLbMfW79A-8tQV9G8PA","PLARIDEL":"1QKqYm8K9c7SYo0sUF9hRjxYQyYKuB5C5i6Hjlup8OX0","POLILLO":"13fb7V-0Yp9fC7a1TIV9B1FkM2beYlya18-fc98nGkSc","QUEZON":"1DqUn0dHinApKBPenSza_ROgpCSyUgd3fqLJo1WiCbK0","REAL":"1TU4ylOiqj4f9Nt2_uH2F3AvnYlftCCjD68DnThqa1Zc","SAMPALOC":"1D1VPC8Mvtyxu_xfMrmOkHvSWBfVMwFd1V4LRnys37sM","SAN ANDRES":"1tHXopLButjlKCDFMDYrC_G7rqFD3GpqH3Lq7YfQGJb8","SAN ANTONIO":"11ExXli4PPXziGTsdbUEIiuFrCXWBmvzjgM17WRrjDpg","SAN FRANCISCO":"1qbanSNREqVOeR_Dju4kRUr-fZ-bWcbb409HQRHuPX88","SAN NARCISO":"1qkUlAmzpyPiY1lhn5qNgl-vsU7cXuS9xZZ0iRR-rIuo","SARIAYA":"1lLujd4ef7kVvmDDKfc1jWjBrQYsMy_mxmd27ZY8oJds","TAGKAWAYAN":"1pqKzmdTytLBZDnQUg3fqH2HIRde5GWakrRSl7rntGKk","TAYABAS CITY":"1djOGtLLIg9imWeUAiT7kAmTs8R8LC-oV7QMh55W-zRA","TIAONG":"1uFaVdeYDRoUhXFegIuf-z1h_-M4GMClLViehJJdOFxE","UNISAN":"1LrtY4yQCa1taOoPE5ZEuz9tS9KXs041pb-HZYo-9lbk","QUEZON PROVINCE":"1HkxTQIjU1OgkVZC40tBsiYJYV0JOsPKOxWDqNg4dr_Y","ACEBA SCIENCE AND TECHNOLOGY INSTITUTE-MAUBAN":"1nhTZUpGVK9bLcFhqU_3wHxr2sd8nPu6ScJoXO-OKzNo","MANUEL S. ENVERGA UNIVERSITY FOUNDATION":"1M5w-kovNAHnPOG2oIgoTdd0-o4r4z9qSeWQ5wGVAflM","SOUTHERN LUZON STATE UNIVERSITY-LUCBAN":"1ZlrFkpETL-ki7BCrmtARL9E7P-jvyX_WejLWoqX6ibM"},"RIZAL":{"ANGONO":"1z7VO4Xmda2eM1I3PAQZkqvBs-0mMObyFnSyf7v-xk1M","CITY OF ANTIPOLO":"1-QC_OVphu1gTCa7ox4JzlJMbepQcuZLOLfhC11bFDP8","BARAS":"1sph8SzdmFmzFhAS0PUDIMMNvD4eQsCEUvqpIkXBK_4k","BINANGONAN":"1Wh4E70k6i-KqGT-UsPdrkXrAKCjCCkgm2xHiK9x-LFs","CAINTA":"1nbCj5UgVkJwh_dHcDrS_pCKdWd8jZ6i1qoIJywPVHNY","CARDONA":"1Or3LR_ycc-QgKPsLerVBGeztkaCoQq5tcKcN7YHCAWc","JALAJALA":"1wOg9VrKRZ1ZNocaXYjg7MCYg01YqQxwSdMMkwEsUQmY","MORONG":"1uIMBedHcQgEHIQYHB3Oq8uoWEigmqIIy2k_sYE9n2tU","PILILLA":"1n6qsneOhR8eD97cSC9O-YcgOuGIfOZpip8EeFfZGfPY","RODRIGUEZ":"1ilSvv6hqEeU6XjfOj8mPZiFmWkbEwMhsaRV0UXBAb60","SAN MATEO":"18SgRviHq9lrERKZReR4ZIlvA22n7cmVNkGik4dY5Op0","TANAY":"1LlH89pymIrRro-ntFlmv0irR_Kj105HWTuO_mfHikWQ","TAYTAY":"12mJIgvJ9-ZjXldUDbaYQ1pSTVqGM3qAIUl5-nCNJcSQ","TERESA":"1oTBxI213uV2EthIZ1_HruZ58dDjBwyK1DAoP58mBCxU","RIZAL PROVINCE":"1UzyOgP56oJuKwebZBG-AtQ7XuxTmz5n8KzlhRaisxK4"}};

    // TTL cache + in-flight request deduplication; bounded to avoid unbounded RAM growth.
    const SPRS_DIRECT_CACHE = new Map();
    const SPRS_DIRECT_PENDING = new Map();
    // Bound upstream concurrency across all visitors, not just within one report.
    const SPRS_UPSTREAM_LIMIT = Math.max(1, Math.min(16, Number(process.env.SPRS_UPSTREAM_LIMIT) || 6));
    let sprsUpstreamActive = 0;
    const sprsUpstreamQueue = [];
    async function sprsWithUpstreamSlot(work) {
      if (sprsUpstreamActive >= SPRS_UPSTREAM_LIMIT) {
        await new Promise(resolve => sprsUpstreamQueue.push(resolve));
      }
      sprsUpstreamActive++;
      try { return await work(); }
      finally {
        sprsUpstreamActive--;
        const next = sprsUpstreamQueue.shift();
        if (next) next();
      }
    }

    const SPRS_CACHE_TTL_MS = Math.max(10000, Number(process.env.SPRS_CACHE_TTL_MS) || 120000);
    const SPRS_CACHE_MAX_ENTRIES = 250;
    function sprsCacheGet(key) {
      const entry = SPRS_DIRECT_CACHE.get(key);
      if (!entry) return null;
      if (entry.expires <= Date.now()) { SPRS_DIRECT_CACHE.delete(key); return null; }
      SPRS_DIRECT_CACHE.delete(key);
      SPRS_DIRECT_CACHE.set(key, entry);
      return entry.value;
    }
    function sprsCacheSet(key, value) {
      SPRS_DIRECT_CACHE.delete(key);
      SPRS_DIRECT_CACHE.set(key, {value, expires: Date.now() + SPRS_CACHE_TTL_MS});
      while (SPRS_DIRECT_CACHE.size > SPRS_CACHE_MAX_ENTRIES) {
        SPRS_DIRECT_CACHE.delete(SPRS_DIRECT_CACHE.keys().next().value);
      }
      return value;
    }


    function sprsDirectNorm(value) {
      return String(value == null ? '' : value).trim().toUpperCase()
        .replace(/[–—]/g, '-')
        .replace(/\s+/g, ' ');
    }

    function sprsDirectMonth(value) {
      const v = sprsDirectNorm(value);
      const months = {
        '1':'JANUARY','01':'JANUARY','JAN':'JANUARY','JANUARY':'JANUARY',
        '2':'FEBRUARY','02':'FEBRUARY','FEB':'FEBRUARY','FEBRUARY':'FEBRUARY',
        '3':'MARCH','03':'MARCH','MAR':'MARCH','MARCH':'MARCH',
        '4':'APRIL','04':'APRIL','APR':'APRIL','APRIL':'APRIL',
        '5':'MAY','05':'MAY','MAY':'MAY',
        '6':'JUNE','06':'JUNE','JUN':'JUNE','JUNE':'JUNE',
        '7':'JULY','07':'JULY','JUL':'JULY','JULY':'JULY',
        '8':'AUGUST','08':'AUGUST','AUG':'AUGUST','AUGUST':'AUGUST',
        '9':'SEPTEMBER','09':'SEPTEMBER','SEP':'SEPTEMBER','SEPT':'SEPTEMBER','SEPTEMBER':'SEPTEMBER',
        '10':'OCTOBER','OCT':'OCTOBER','OCTOBER':'OCTOBER',
        '11':'NOVEMBER','NOV':'NOVEMBER','NOVEMBER':'NOVEMBER',
        '12':'DECEMBER','DEC':'DECEMBER','DECEMBER':'DECEMBER'
      };
      return months[v] || v;
    }

    function sprsDirectMonthMatches(value, requested) {
      const r = sprsDirectMonth(requested);
      return !r || r === 'ALL' || sprsDirectMonth(value) === r;
    }

    function sprsDirectCell(row, index) {
      if (!row) return '';
      const cells = Array.isArray(row.__values) ? row.__values : [];
      return cells[index] == null ? '' : cells[index];
    }

    function sprsDirectDateKey(value) {
      let s = String(value == null ? '' : value).trim();
      if (!s) return '';

      const g = s.match(/^Date\((\d{4}),(\d{1,2}),(\d{1,2})(?:,\d+.*)?\)$/i);
      if (g) return `${g[1]}-${String(Number(g[2]) + 1).padStart(2,'0')}-${String(g[3]).padStart(2,'0')}`;

      const iso = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
      if (iso) return `${iso[1]}-${String(iso[2]).padStart(2,'0')}-${String(iso[3]).padStart(2,'0')}`;

      const slash = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
      if (slash) return `${slash[3]}-${String(slash[1]).padStart(2,'0')}-${String(slash[2]).padStart(2,'0')}`;

      const d = new Date(s);
      if (!Number.isNaN(d.getTime())) {
        return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
      }
      return '';
    }

    function sprsDirectDateText(value) {
      const key = sprsDirectDateKey(value);
      return key || String(value == null ? '' : value).trim();
    }

    function sprsDirectNumber(value) {
      const n = Number(String(value == null ? '' : value).replace(/,/g,'').trim());
      return Number.isFinite(n) ? n : 0;
    }

    function sprsDirectYes(value) {
      const v = sprsDirectNorm(value);
      return ['YES','Y','TRUE','1','HOTS','HIRED ON THE SPOT','HIRED-ON-THE-SPOT'].includes(v);
    }

    function sprsDirectHas(value, words) {
      const v = sprsDirectNorm(value);
      return words.some(w => v.includes(w));
    }

    function sprsDirectParseGviz(text) {
      const start = text.indexOf('{');
      const end = text.lastIndexOf('}');
      if (start < 0 || end <= start) throw new Error('Invalid Google Sheets response.');
      const payload = JSON.parse(text.slice(start, end + 1));
      const cols = (payload.table && payload.table.cols) || [];
      const rows = (payload.table && payload.table.rows) || [];

      return rows.map(row => {
        const values = [];
        const cells = row && Array.isArray(row.c) ? row.c : [];
        for (let i = 0; i < Math.max(cols.length, cells.length); i++) {
          const c = cells[i];
          if (!c) values[i] = '';
          else if (c.f != null) values[i] = c.f;
          else if (c.v != null) values[i] = c.v;
          else values[i] = '';
        }
        return { __values: values };
      });
    }

    async function sprsDirectFetchSheet(id, sheetName, range) {
      const key = `${id}|${sheetName}|${range}`;
      const cached = sprsCacheGet(key);
      if (cached !== null) return cached;
      if (SPRS_DIRECT_PENDING.has(key)) return SPRS_DIRECT_PENDING.get(key);
      const pending = (async () => {
        const url = 'https://docs.google.com/spreadsheets/d/' + encodeURIComponent(id) +
          '/gviz/tq?tqx=out:json&headers=0&sheet=' + encodeURIComponent(sheetName) +
          '&range=' + encodeURIComponent(range);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 20000);
        try {
          return await sprsWithUpstreamSlot(async () => {
            const response = await fetch(url, { method:'GET', headers:{'Accept':'text/plain'}, redirect:'follow', signal:controller.signal });
            if (!response.ok) throw new Error(`${sheetName}: Google returned HTTP ${response.status}`);
            const body = await response.text();
            if (/<!doctype html|<html/i.test(body.slice(0,500))) {
              throw new Error(`${sheetName}: Google Sheet is not publicly readable by the server.`);
            }
            return sprsCacheSet(key, sprsDirectParseGviz(body));
          });
        } finally { clearTimeout(timeout); }
      })();
      SPRS_DIRECT_PENDING.set(key, pending);
      try { return await pending; }
      finally { SPRS_DIRECT_PENDING.delete(key); }
    }

    function sprsDirectNormalizeJf(row, source) {
      const year = String(sprsDirectCell(row,0) || '').trim();
      const month = String(sprsDirectCell(row,1) || '').trim();
      const clearanceNo = String(sprsDirectCell(row,3) || '').trim();
      const date = sprsDirectDateText(sprsDirectCell(row,4));
      const venue = String(sprsDirectCell(row,5) || '').trim();

      const participatingLocal = sprsDirectNumber(sprsDirectCell(row,6));
      const participatingGovernment = sprsDirectNumber(sprsDirectCell(row,7));
      const participatingOverseas = sprsDirectNumber(sprsDirectCell(row,8));
      const vacanciesPrivate = sprsDirectNumber(sprsDirectCell(row,9));
      const vacanciesGovernment = sprsDirectNumber(sprsDirectCell(row,10));
      const vacanciesOverseas = sprsDirectNumber(sprsDirectCell(row,11));

      const comp_loc = participatingLocal + participatingGovernment;
      const comp_ovs = participatingOverseas;
      const vac_loc = vacanciesPrivate + vacanciesGovernment;
      const vac_ovs = vacanciesOverseas;

      return {
        id: `jf-${source.province}-${source.municipality}-${clearanceNo}-${date}`,
        rowIndex: '',
        province: source.province,
        municipality: source.municipality,
        year,
        reportingPeriod: month,
        lpoNo: clearanceNo || `${source.municipality} JF`,
        dateReceived: '',
        dateIssued: '',
        organizer: '',
        date,
        venue,
        documentApplied: '',
        actionTaken: '',
        metrics: {
          comp_tot: comp_loc + comp_ovs,
          comp_loc,
          comp_ovs,
          vac_tot: vac_loc + vac_ovs,
          vac_loc,
          vac_ovs,
          app_tot:0, app_m:0, app_f:0,
          hots_tot:0, hots_m:0, hots_f:0,
          qual_tot:0, qual_m:0, qual_f:0,
          near_tot:0, near_m:0, near_f:0,
          unqual_tot:0, unqual_m:0, unqual_f:0,
          placed_30:0, placed_m:0, placed_f:0
        },
        originalRecord: { province:source.province, municipality:source.municipality, year, month, clearanceNo, date, venue }
      };
    }

    // ============================================================================
    // REG_APPLICANTS COLUMN MAP (A:Y)
    // IMPORTANT: Array indexes are zero-based.
    // A=0 YEAR, B=1 MONTH, C=2 PROVINCE, D=3 LAST NAME, E=4 FIRST NAME,
    // F=5 MIDDLE NAME, I=8 AGE, J=9 SEX/GENDER, P=15 DATE OF JOB FAIR,
    // Q=16 INTERVIEWED, R=17 HOTS, U=20 STATUS OF APPLICATION,
    // V=21 PLACEMENT, W=22 DATE PLACED, Y=24 CLEARANCE.
    // ============================================================================
    const SPRS_REG_COL = Object.freeze({
      YEAR: 0,
      MONTH: 1,
      PROVINCE: 2,
      LAST_NAME: 3,
      FIRST_NAME: 4,
      MIDDLE_NAME: 5,
      AGE: 8,
      GENDER: 9,
      DATE_OF_JOB_FAIR: 15,
      INTERVIEWED: 16,
      HOTS: 17,
      STATUS_OF_APPLICATION: 20,
      PLACEMENT: 21,
      DATE_PLACED: 22,
      CLEARANCE: 24
    });

    function sprsDirectGender(value) {
      const v = sprsDirectNorm(value);

      // Check FEMALE before MALE because FEMALE contains the text MALE.
      if (v === 'F' || v === 'FEMALE' || v.includes('FEMALE')) return 'F';
      if (v === 'M' || v === 'MALE' || v.includes('MALE')) return 'M';
      return '';
    }

    function sprsDirectRegistryMatches(registryRows, jf) {
      const clearance = sprsDirectNorm(jf.lpoNo);
      const jfDate = sprsDirectDateKey(jf.date);
      const jfMonth = sprsDirectMonth(jf.reportingPeriod);
      const jfYear = sprsDirectNorm(jf.year);

      return registryRows.filter(row => {
        const name = [2,3,4].map(i => String(sprsDirectCell(row,i)||'').trim()).join('');
        if (!name) return false;

        const rowClearance = sprsDirectNorm(sprsDirectCell(row, SPRS_REG_COL.CLEARANCE));
        const rowDate = sprsDirectDateKey(sprsDirectCell(row, SPRS_REG_COL.DATE_OF_JOB_FAIR));
        const rowMonth = sprsDirectMonth(sprsDirectCell(row, SPRS_REG_COL.MONTH));
        const rowYear = sprsDirectNorm(sprsDirectCell(row, SPRS_REG_COL.YEAR));

        if (clearance && rowClearance && clearance === rowClearance) return true;
        if (jfDate && rowDate && jfDate === rowDate) return true;
        if (!rowClearance && !rowDate && jfMonth && rowMonth === jfMonth && (!jfYear || rowYear === jfYear)) return true;
        return false;
      });
    }

    function sprsDirectBuildRegistryMetrics(rows, jf) {
      const out = {
        app_tot:0, app_m:0, app_f:0,
        hots_tot:0, hots_m:0, hots_f:0,
        qual_tot:0, qual_m:0, qual_f:0,
        near_tot:0, near_m:0, near_f:0,
        unqual_tot:0, unqual_m:0, unqual_f:0,
        placed_30:0, placed_m:0, placed_f:0
      };
      const jfDate = sprsDirectDateKey(jf.date);
      const jfTime = jfDate ? new Date(`${jfDate}T00:00:00`) : null;

      rows.forEach(row => {
        const name = [2,3,4].map(i => String(sprsDirectCell(row,i)||'').trim()).join('');
        if (!name) return;

        out.app_tot++;
        // J / index 9 = SEX/GENDER (NOT I / index 8 = AGE).
        const sex = sprsDirectGender(sprsDirectCell(row, SPRS_REG_COL.GENDER));
        const male = sex === 'M' || sex === 'MALE';
        const female = sex === 'F' || sex === 'FEMALE';
        if (male) out.app_m++;
        if (female) out.app_f++;

        const hots = sprsDirectCell(row, SPRS_REG_COL.HOTS);
        if (sprsDirectYes(hots)) {
          out.hots_tot++; if (male) out.hots_m++; if (female) out.hots_f++;
        }

        // U / index 20 = STATUS OF APPLICATION.
        const status = sprsDirectNorm(sprsDirectCell(row, SPRS_REG_COL.STATUS_OF_APPLICATION));
        if (sprsDirectHas(status,['QUALIFIED']) && !sprsDirectHas(status,['UNQUALIFIED','NOT QUALIFIED'])) {
          out.qual_tot++; if (male) out.qual_m++; if (female) out.qual_f++;
        }
        if (sprsDirectHas(status,['NEAR HIRE','NEAR-HIRE','NEAR HIRED','NEAR-HIRED','NEAR'])) {
          out.near_tot++; if (male) out.near_m++; if (female) out.near_f++;
        }
        if (sprsDirectHas(status,['UNQUALIFIED','MISMATCH','NOT QUALIFIED'])) {
          out.unqual_tot++; if (male) out.unqual_m++; if (female) out.unqual_f++;
        }

        const placement = sprsDirectNorm(sprsDirectCell(row, SPRS_REG_COL.PLACEMENT));
        const datePlaced = sprsDirectDateKey(sprsDirectCell(row, SPRS_REG_COL.DATE_PLACED));
        if ((sprsDirectHas(placement,['PLACED','HIRED']) || datePlaced) && jfTime && datePlaced) {
          const placedTime = new Date(`${datePlaced}T00:00:00`);
          const days = (placedTime - jfTime) / 86400000;
          if (days >= 0 && days <= 30) {
            out.placed_30++; if (male) out.placed_m++; if (female) out.placed_f++;
          }
        }
      });
      return out;
    }

    async function sprsDirectLoadMunicipality(province, municipality, id, selectedMonth) {
      const jfRows = await sprsDirectFetchSheet(id, 'JF', 'A:O');
      const reports = [];

      for (const row of jfRows) {
        const year = String(sprsDirectCell(row,0)||'').trim();
        const month = String(sprsDirectCell(row,1)||'').trim();
        const clearance = String(sprsDirectCell(row,3)||'').trim();
        const date = sprsDirectCell(row,4);

        if (!/^\d{4}$/.test(year)) continue;
        if (!clearance && !date) continue;
        if (!sprsDirectMonthMatches(month, selectedMonth)) continue;

        reports.push(sprsDirectNormalizeJf(row,{province,municipality}));
      }

      if (!reports.length) return reports;

      let registryRows = [];
      try {
        registryRows = await sprsDirectFetchSheet(id, 'REG_APPLICANTS', 'A:Y');
      } catch (e) {
        // Registry data is supplementary. Keep the JF report even if the registry tab
        // is unavailable; the response will still contain the JF/company/vacancy data.
      }

      reports.forEach(report => {
        if (registryRows.length) {
          const matched = sprsDirectRegistryMatches(registryRows, report);
          report.metrics = { ...report.metrics, ...sprsDirectBuildRegistryMetrics(matched, report) };
        }
      });

      return reports;
    }

    async function sprsDirectBuildReport(province, month) {
      const key = Object.keys(SPRS_GOOGLE_SHEET_DATABASE).find(k => sprsDirectNorm(k) === sprsDirectNorm(province));
      if (!key) return { reports:[], errors:[`No Google Sheet mapping exists for ${province}.`] };

      const sources = Object.entries(SPRS_GOOGLE_SHEET_DATABASE[key] || {}).map(([municipality,id]) => ({ municipality, id }));
      const reports = [];
      const errors = [];
      const batchSize = 6;

      for (let i=0; i<sources.length; i+=batchSize) {
        const batch = sources.slice(i,i+batchSize);
        const results = await Promise.all(batch.map(async source => {
          try {
            return { source, rows: await sprsDirectLoadMunicipality(key, source.municipality, source.id, month), error:null };
          } catch (error) {
            return { source, rows:[], error };
          }
        }));
        results.forEach(r => {
          if (r.rows.length) reports.push(...r.rows);
          if (r.error) errors.push(`${key} / ${r.source.municipality}: ${r.error.message || r.error}`);
        });
      }

      reports.sort((a,b) => {
        const da = sprsDirectDateKey(a.date), db = sprsDirectDateKey(b.date);
        return da && db ? da.localeCompare(db) : String(a.lpoNo).localeCompare(String(b.lpoNo));
      });
      return { reports, errors, searchIndex: reports.map(row => JSON.stringify(row).toLocaleLowerCase()) };
    }

    // Optional server-side pagination and filtering; legacy callers receive the original response.
    // Example: /api/job-fair/sprs-google?province=CAVITE&month=ALL&page=1&pageSize=50&search=abc
    const SPRS_REPORT_CACHE = new Map();
    const SPRS_REPORT_PENDING = new Map();
    async function sprsCachedReport(province, month) {
      const key = sprsDirectNorm(province) + '|' + sprsDirectNorm(month);
      const item = SPRS_REPORT_CACHE.get(key);
      if (item && item.expires > Date.now()) return item.value;
      if (SPRS_REPORT_PENDING.has(key)) return SPRS_REPORT_PENDING.get(key);
      const pending = sprsDirectBuildReport(province, month);
      SPRS_REPORT_PENDING.set(key, pending);
      try {
        const value = await pending;
        SPRS_REPORT_CACHE.set(key, {value, expires: Date.now() + SPRS_CACHE_TTL_MS});
        while (SPRS_REPORT_CACHE.size > 20) SPRS_REPORT_CACHE.delete(SPRS_REPORT_CACHE.keys().next().value);
        return value;
      } finally { SPRS_REPORT_PENDING.delete(key); }
    }
    // Reusable dashboard aggregate: computes once per cached report, not per table page.
    const sprsDashboardSummaryCache = new Map();
    app.get('/api/job-fair/sprs-dashboard-summary', async (req, res) => {
      try {
        const province = String(req.query.province || 'ALL').trim();
        const month = String(req.query.month || 'ALL').trim();
        if (sprsDirectNorm(province) === 'ALL') return res.json({success:true,total:0,province,month});
        const key = JSON.stringify([province.toUpperCase(),month.toUpperCase(),sheetCacheGeneration]);
        const hit = sprsDashboardSummaryCache.get(key);
        if (hit && hit.expires > Date.now()) return res.json(hit.data);
        const result = await sprsCachedReport(province,month);
        const data = {success:true,province,month,total:result.reports.length,errors:result.errors};
        sprsDashboardSummaryCache.set(key,{data,expires:Date.now()+Math.max(5000,SHEET_READ_TTL_MS)});
        while(sprsDashboardSummaryCache.size>80) sprsDashboardSummaryCache.delete(sprsDashboardSummaryCache.keys().next().value);
        return res.json(data);
      } catch(err) { return res.status(500).json({success:false,message:err.message}); }
    });
    app.get('/api/job-fair/sprs-google', async (req, res) => {
      try {
        const province = String(req.query.province || 'ALL').trim();
        const month = String(req.query.month || 'ALL').trim();
        const paged = req.query.page !== undefined || req.query.pageSize !== undefined || req.query.search !== undefined || req.query.summary === 'true';
        if (!province || sprsDirectNorm(province) === 'ALL') {
          return res.json({success:true, province, month, count:0, data:[], records:[], ...(paged ? {total:0,page:1,pageSize:50,totalPages:0,summary:{total:0}} : {})});
        }
        const result = await sprsCachedReport(province, month);
        if (!paged) return res.json({success:true, province, month, count:result.reports.length, data:result.reports, records:result.reports, errors:result.errors});
        const page = Math.max(1, Math.min(1000000, parseInt(req.query.page, 10) || 1));
        const pageSize = Math.max(1, Math.min(200, parseInt(req.query.pageSize, 10) || 50));
        const search = String(req.query.search || '').trim().toLocaleLowerCase().slice(0,200);
        const filtered = search ? result.reports.filter((row, index) => result.searchIndex[index].includes(search)) : result.reports;
        const total = filtered.length;
        const totalPages = Math.ceil(total/pageSize);
        const data = req.query.summary === 'true' ? [] : filtered.slice((page-1)*pageSize,page*pageSize);
        return res.json({success:true,province,month,count:total,total,page,pageSize,totalPages,data,records:data,errors:result.errors,summary:{total,unfilteredTotal:result.reports.length}});
      } catch (err) {
        console.error('SPRS Google Sheets error:', err);
        return res.status(500).json({success:false,message:err.message || 'Failed to load SPRS Google Sheets.',data:[],records:[]});
      }
    });

    // ============================================================================
    // SPRS CONSOLIDATED JOB FAIR REPORT
    // ============================================================================
    // Example:
    //
    // /api/job-fair/consolidated?province=CAVITE&month=JANUARY
    //
    // /api/job-fair/consolidated?province=ALL&month=ALL
    // ============================================================================
    app.get(
      '/api/job-fair/consolidated',
      (req, res) => {

        try {

          const requestedProvince =
            normalizeText(
              req.query.province ||
              'ALL'
            );


          const requestedMonth =
            normalizeMonth(
              req.query.month ||
              'ALL'
            );


          const provinceUpper =
            requestedProvince
              .toUpperCase();


          const allRecords =
            getJfRecords();


          const normalizedRecords =
            allRecords.map(
              normalizeJfRecordForSPRS
            );


          const filtered =
            normalizedRecords.filter(
              record => {

                // ---------------------------------------------------------------
                // PROVINCE FILTER
                // ---------------------------------------------------------------
                if (
                  provinceUpper &&
                  provinceUpper !== 'ALL' &&
                  provinceUpper !== 'ALL PROVINCES'
                ) {

                  const recordProvince =
                    normalizeProvince(
                      record.province ||
                      record.fieldOffice
                    );


                  if (
                    recordProvince !==
                    provinceUpper
                  ) {
                    return false;
                  }
                }


                // ---------------------------------------------------------------
                // MUNICIPALITY / CITY FILTER
                // ---------------------------------------------------------------
                const requestedMunicipality =
                  normalizeText(
                    req.query.municipality ||
                    'ALL'
                  );

                if (
                  requestedMunicipality &&
                  requestedMunicipality.toUpperCase() !== 'ALL' &&
                  requestedMunicipality.toUpperCase() !== 'ALL MUNICIPALITIES'
                ) {

                  const source =
                    record.originalRecord || {};

                  const recordMunicipality =
                    normalizeText(
                      firstValue(
                        source,
                        [
                          'municipality','Municipality','municipalityCity',
                          'municipality_city','city','cityMunicipality',
                          'city_municipality'
                        ],
                        ''
                      )
                    ).toUpperCase();

                  if (
                    recordMunicipality !==
                    requestedMunicipality.toUpperCase()
                  ) {
                    return false;
                  }
                }

                // ---------------------------------------------------------------
                // MONTH FILTER
                // ---------------------------------------------------------------
                if (
                  requestedMonth &&
                  requestedMonth !== 'ALL'
                ) {

                  const dateMonth =
                    getMonthNameFromDate(
                      record.date
                    );


                  const reportingMonth =
                    normalizeMonth(
                      record.reportingPeriod
                    );


                  const matchesDate =
                    dateMonth ===
                    requestedMonth;


                  const matchesReportingPeriod =
                    reportingMonth ===
                    requestedMonth;


                  if (
                    !matchesDate &&
                    !matchesReportingPeriod
                  ) {
                    return false;
                  }
                }


                return true;
              }
            );


          return res.json({

            success: true,

            province:
              requestedProvince,

            month:
              requestedMonth,

            count:
              filtered.length,

            data:
              filtered,

            records:
              filtered

          });

        } catch (err) {

          console.error(
            'Error in consolidated SPRS report:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              err.message ||
              'Failed to load consolidated Job Fair report.',

            data: [],

            records: []

          });

        }
      }
    );


    // ============================================================================
    // UPLOAD ATTACHMENT HELPER
    // ============================================================================
    function uploadAttachmentToDrive(
      base64Data,
      fileName,
      mimeType,
      folderType
    ) {

      const defaultDriveUrl =
        (
          String(folderType || '')
            .toUpperCase()
            .includes('RECEIV')
        )
          ? PROOF_RECEIVED_DRIVE_URL
          : PROOF_RELEASED_DRIVE_URL;


      try {

        if (!base64Data) {
          return defaultDriveUrl;
        }


        if (
          typeof base64Data === 'string' &&
          (
            base64Data.startsWith(
              'http://'
            ) ||
            base64Data.startsWith(
              'https://'
            )
          )
        ) {
          return base64Data;
        }


        const uploadsDir =
          path.join(
            __dirname,
            'public',
            'uploads'
          );


        if (
          !fs.existsSync(
            uploadsDir
          )
        ) {

          fs.mkdirSync(
            uploadsDir,
            {
              recursive: true
            }
          );
        }


        const splitData =
          base64Data.split(',');


        const pureData =
          splitData.length > 1
            ? splitData[1]
            : splitData[0];


        const buffer =
          Buffer.from(
            pureData,
            'base64'
          );


        const safeFileName =
          `${Date.now()}_${(
            fileName ||
            'attachment'
          ).replace(
            /[^a-zA-Z0-9_.-]/g,
            '_'
          )}`;


        const filePath =
          path.join(
            uploadsDir,
            safeFileName
          );


        fs.writeFileSync(
          filePath,
          buffer
        );


        return `/uploads/${safeFileName}`;

      } catch (error) {

        console.error(
          "Error in uploadAttachmentToDrive:",
          error.message
        );

        return defaultDriveUrl;
      }
    }


    // ============================================================================
    // REGISTER USER API
    // ============================================================================
    // Browser -> Express -> Google Apps Script.
    // Keeping the Apps Script URL server-side prevents the dashboard from trying
    // to parse Google HTML/redirect pages as JSON.
    app.post('/api/register-user', async (req, res) => {
      try {
        const username = String((req.body && req.body.username) || '').trim();
        const email = String((req.body && req.body.email) || '').trim().toLowerCase();
        const password = String((req.body && req.body.password) || '');

        if (!username) {
          return res.status(400).json({ success: false, message: 'Username is required.' });
        }

        if (!email) {
          return res.status(400).json({ success: false, message: 'Email address is required.' });
        }

        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
          return res.status(400).json({ success: false, message: 'A valid email address is required.' });
        }

        if (!password) {
          return res.status(400).json({ success: false, message: 'Password is required.' });
        }

        const result = await callAuthSheetApi({
          action: 'registeruser',
          username,
          password,
          email
        });

        if (!result || result.success !== true) {
          return res.status(400).json({
            success: false,
            message: (result && result.message) || 'Unable to register user.'
          });
        }

        return res.json({
          success: true,
          message: result.message || 'User registered successfully.'
        });
      } catch (error) {
        console.error('Register user API error:', error);
        return res.status(500).json({
          success: false,
          message: error && error.message ? error.message : 'Unable to register user.'
        });
      }
    });


    // ============================================================================
    // PROGRAM ACCESS MANAGEMENT API - GIP / SPES / SPRS / OTHER PROGRAMS
    // ============================================================================
    const PROGRAM_ACCESS_FILE = 'programAccess.json';
    const PROGRAM_ACCESS_ADMINS = new Set(['PAUL','JM','CIS','FRANCE','FRANCIS','DOLE4A']);
    const DEFAULT_GIP_USERS = new Set([
      'PAUL','JM','CIS','FRANCE','FRANCIS','DOLE4A',
      'SHERWIN@BATANGAS','ALIYAH@LAGUNA','JESS@CAVITE',
      'JERICHO@RIZAL','MARICON@QUEZON'
    ]);

    function normalizeProgramUsername(value) {
      return String(value || '').trim().toUpperCase().replace(/\s+/g, ' ');
    }

    function getProgramAccessStore() {
      const data = loadJsonData(PROGRAM_ACCESS_FILE, {});
      return data && typeof data === 'object' && !Array.isArray(data) ? data : {};
    }

    function getEffectiveProgramAccess(username) {
      const key = normalizeProgramUsername(username);
      const store = getProgramAccessStore();
      const saved = store[key] && typeof store[key] === 'object' ? store[key] : {};
      return {
        gip: typeof saved.gip === 'boolean' ? saved.gip : DEFAULT_GIP_USERS.has(key),
        spes: saved.spes === true,
        sprs: saved.sprs === true,
        otherPrograms: saved.otherPrograms === true
      };
    }

    app.get('/api/program-access', async (req, res) => {
      try {
        const username = normalizeProgramUsername(req.query && req.query.username);
        if (!username) return res.status(400).json({success:false,message:'Username is required.'});

        const url = new URL(AUTH_SHEET_API_URL);
        url.searchParams.set('action','getprogramaccess');
        url.searchParams.set('username',username);
        url.searchParams.set('_',Date.now().toString());

        const upstream = await fetch(url.toString(), {
          method:'GET', redirect:'follow', headers:{'Accept':'application/json'}
        });
        const text = await upstream.text();
        let result;
        try { result=JSON.parse(text); }
        catch(e) { throw new Error('Program access database returned invalid JSON.'); }

        if (!upstream.ok || !result || result.success !== true) {
          return res.status(upstream.ok?400:502).json({
            success:false,message:(result&&result.message)||'Unable to load program access.'
          });
        }

        return res.json({
          success:true,username,
          access:{
            gip:result.access&&result.access.gip===true,
            spes:result.access&&result.access.spes===true,
            sprs:result.access&&result.access.sprs===true,
            otherPrograms:result.access&&result.access.otherPrograms===true
          }
        });
      } catch(error) {
        console.error('Program access GET error:',error);
        return res.status(500).json({success:false,message:error.message||'Unable to load program access.'});
      }
    });

    app.post('/api/program-access', async (req, res) => {
      try {
        const requestUser=normalizeProgramUsername(req.body&&req.body.requestUser);
        const username=normalizeProgramUsername(req.body&&req.body.username);
        const access=req.body&&req.body.access;

        if (!PROGRAM_ACCESS_ADMINS.has(requestUser)) {
          return res.status(403).json({success:false,message:'You are not authorized to manage program access.'});
        }
        if (!username) return res.status(400).json({success:false,message:'Username is required.'});
        if (!access || typeof access!=='object') {
          return res.status(400).json({success:false,message:'Access settings are required.'});
        }

        const result=await callAuthSheetApi({
          action:'setprogramaccess',requestUser,username,
          access:{
            gip:access.gip===true,
            spes:access.spes===true,
            sprs:access.sprs===true,
            otherPrograms:access.otherPrograms===true
          }
        });

        if (!result || result.success!==true) {
          return res.status(400).json({success:false,message:(result&&result.message)||'Unable to save program access.'});
        }

        return res.json({
          success:true,username,
          access:{
            gip:result.access&&result.access.gip===true,
            spes:result.access&&result.access.spes===true,
            sprs:result.access&&result.access.sprs===true,
            otherPrograms:result.access&&result.access.otherPrograms===true
          },
          message:result.message||'Program access updated successfully.'
        });
      } catch(error) {
        console.error('Program access POST error:',error);
        return res.status(500).json({success:false,message:error.message||'Unable to save program access.'});
      }
    });

    // ============================================================================
    // VIEW USERS EMAIL API
    // ============================================================================
    // Browser -> Express -> Google Apps Script GET endpoint.
    // Always converts the upstream response into JSON for the Dashboard.
    app.get('/api/users-email', async (req, res) => {
      try {
        const requestUser = String((req.query && req.query.requestUser) || '').trim();

        if (!requestUser) {
          return res.status(400).json({
            success: false,
            message: 'Requesting user is required.'
          });
        }

        const url = new URL(AUTH_SHEET_API_URL);
        url.searchParams.set('action', 'getusersemail');
        url.searchParams.set('requestUser', requestUser);

        const upstream = await fetch(url.toString(), {
          method: 'GET',
          redirect: 'follow',
          headers: { 'Accept': 'application/json' }
        });

        const text = await upstream.text();
        let result;

        try {
          result = JSON.parse(text);
        } catch (parseError) {
          console.error('Users email API returned non-JSON:', text.slice(0, 500));
          return res.status(502).json({
            success: false,
            message: 'User database returned an invalid response. Redeploy Code.gs as a Web App and verify the /exec URL.'
          });
        }

        if (!upstream.ok || !result || result.success !== true) {
          return res.status(upstream.ok ? 400 : 502).json({
            success: false,
            message: (result && result.message) || 'Unable to load users email.'
          });
        }

        const users = Array.isArray(result.users) ? result.users : [];

        return res.json({
          success: true,
          users,
          count: users.length,
          source: 'AUTH_SHEET_API'
        });
      } catch (error) {
        console.error('Users email API error:', error);
        return res.status(500).json({
          success: false,
          message: error && error.message ? error.message : 'Unable to load users email.'
        });
      }
    });


    // ============================================================================
    // DEDICATED FILE UPLOAD API
    // ============================================================================
    app.post(
      '/api/jf/upload',
      (req, res) => {

        try {

          const {
            base64Data,
            fileName,
            mimeType,
            folderType
          } = req.body || {};


          const driveUrl =
            (
              String(folderType || '')
                .toUpperCase()
                .includes('RECEIV')
            )
              ? PROOF_RECEIVED_DRIVE_URL
              : PROOF_RELEASED_DRIVE_URL;


          const folderId =
            (
              String(folderType || '')
                .toUpperCase()
                .includes('RECEIV')
            )
              ? PROOF_RECEIVED_FOLDER_ID
              : PROOF_RELEASED_FOLDER_ID;


          let fileUrl =
            driveUrl;


          if (base64Data) {

            fileUrl =
              uploadAttachmentToDrive(
                base64Data,
                fileName,
                mimeType,
                folderType
              );
          }


          return res.json({

            success: true,

            fileUrl,

            driveUrl,

            folderId

          });

        } catch (err) {

          console.error(
            "Error in /api/jf/upload:",
            err
          );

          return res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // TAB 4 "MARK AS DONE" DRIVE UPLOAD API
    // ============================================================================
    // Real Google Drive upload via the APPLICATION Apps Script web app.
    // Configure APPLICATION_APPS_SCRIPT_URL on Render with the deployed /exec URL.
    // Application attachments: relay to the deployed Google Apps Script web app.
    // APPLICATION_APPS_SCRIPT_URL in Render overrides the known deployment URL.
    const DEFAULT_APPLICATION_APPS_SCRIPT_URL =
      'https://script.google.com/macros/s/AKfycby34e6jK0WfoN0Cu91lvEL-kgj6geFt3J6kxXj8R9YpWfavk7iq4IfVNL0E14wzJqcqEw/exec';

    app.post('/api/jf/upload-drive', async (req, res) => {
      try {
        const scriptUrl = String(process.env.APPLICATION_APPS_SCRIPT_URL || DEFAULT_APPLICATION_APPS_SCRIPT_URL).trim();
        if (!/^https:\/\/script\.google\.com\/macros\/s\/[^/]+\/exec(?:\?.*)?$/i.test(scriptUrl)) {
          return res.status(503).json({ success:false, message:'Invalid APPLICATION_APPS_SCRIPT_URL. Configure the deployed APPLICATION Code.gs /exec URL in Render.' });
        }
        const body = req.body || {};
        const type = String(body.attachmentType || '').trim().toUpperCase();
        const allowed = new Set(['PROOF_RECEIVED','PROOF_RELEASED','PROOF_ISSUED','PARTICIPATING_ENTITIES','JOB_VACANCIES','SIGNED_APPLICATION_FORM','UPDATED_JF_FORM']);
        if (!allowed.has(type)) return res.status(400).json({success:false,message:'INVALID ATTACHMENT TYPE: '+type});
        const dataUrl = String(body.dataUrl || '');
        if (!/^data:[^;]+;base64,[A-Za-z0-9+/=\s]+$/i.test(dataUrl)) {
          return res.status(400).json({success:false,message:'Invalid or missing base64 attachment data.'});
        }
        const upstream = await fetch(scriptUrl, {
          method:'POST',
          headers:{'Content-Type':'text/plain;charset=utf-8', 'Accept':'application/json'},
          redirect:'follow',
          body:JSON.stringify({
            action:'uploadAttachment', attachmentType:type,
            applicationId:body.applicationId || '', documentNumber:body.documentNumber || '',
            fileName:body.fileName || 'ATTACHMENT', mimeType:body.mimeType || '', dataUrl
          }),
          signal:AbortSignal.timeout(90000)
        });
        const raw = await upstream.text();
        const contentType = String(upstream.headers.get('content-type') || '');
        let result;
        try { result = JSON.parse(raw); }
        catch (_) {
          const isHtml = /^\s*(?:<!doctype\s+html|<html|<head|<body)/i.test(raw) || /text\/html/i.test(contentType);
          console.error('APPLICATION Apps Script non-JSON response:', {
            status:upstream.status, contentType, url:upstream.url,
            preview:raw.slice(0,300)
          });
          return res.status(502).json({
            success:false,
            message:isHtml
              ? 'Google Apps Script returned an HTML page instead of JSON. Verify the APPLICATION_APPS_SCRIPT_URL deployment, execute-as-Me permissions, Anyone access, and redeploy the latest Code.gs.'
              : 'Google Apps Script returned an invalid response (HTTP '+upstream.status+'). Check the deployment and Render logs.'
          });
        }
        if (!upstream.ok) {
          return res.status(502).json({success:false,message:String(result.message || result.error || 'Google Apps Script HTTP '+upstream.status)});
        }
        if (!result || result.success !== true) {
          return res.status(502).json({success:false,message:String((result && (result.message || result.error)) || 'Google Drive upload failed.')});
        }
        const url = String(result.url || '').trim();
        if (!/^https:\/\/(?:drive|docs)\.google\.com\//i.test(url)) {
          return res.status(502).json({success:false,message:'APPLICATION Apps Script did not return a Google Drive file link.'});
        }
        return res.json({success:true,url,fileId:result.fileId || '',name:result.name || body.fileName || 'ATTACHMENT'});
      } catch (err) {
        console.error('APPLICATION Google Drive upload:',err);
        const timedOut = err && (err.name === 'TimeoutError' || err.name === 'AbortError');
        return res.status(timedOut ? 504 : 502).json({
          success:false,
          message:timedOut ? 'Google Drive upload timed out. Retry with a smaller file or check Apps Script execution logs.' : (err.message || 'Google Drive upload failed.')
        });
      }
    });


    // ============================================================================
    // PAGE CONFIGURATION & RENDERING
    // ============================================================================
    const PAGE_CONFIG = {

      'login': {
        files: [
          'login',
          'Login'
        ],
        title:
          'DOLE Employment Program | CALABARZON'
      },


      'dashboard': {
        files: [
          'Dashboard',
          'dashboard',
          'Index',
          'index'
        ],
        title:
          'Dashboard | CALABARZON'
      },


      'mer': {
        files: [
          'mer',
          'Mer',
          'MER'
        ],
        title:
          'JF MER Summary and Reports | CALABARZON'
      },


      'jfencoding': {
        files: [
          'JFEncoding',
          'JfEncoding',
          'jfencoding',
          'Index',
          'index'
        ],
        title:
          'Job Fairs - Applicants Pre-Registration & Employers Encoding'
      },


      'nationalreports': {
        files: [
          'NationalReports',
          'nationalreports',
          'National_Reports'
        ],
        title:
          'Job Fair Reports | CALABARZON'
      },


      // Keep BLEFORMS compatibility
      'bleforms': {
        files: [
          'BleForms',
          'bleforms',
          'BLEForms'
        ],
        title:
          'BleForms | CALABARZON'
      },


      // UPDATED:
      // SPRS now loads SPRS.html
      'sprs': {
        files: [
          'SPRS',
          'sprs'
        ],
        title:
          'SPRS | CALABARZON'
      },


      'index': {
        files: [
          'Index',
          'index',
          'JFEncoding',
          'jfencoding'
        ],
        title:
          'Job Fairs System | CALABARZON'
      },


      'gip': {
        files: [
          'GIP',
          'gip',
          'Index',
          'index'
        ],
        title:
          'GIP | CALABARZON'
      },


      'spes': {
        files: [
          'SPES',
          'spes',
          'Index',
          'index'
        ],
        title:
          'SPES | CALABARZON'
      },


      'jobfairs': {
        files: [
          'JFEncoding',
          'jfencoding',
          'Index',
          'index'
        ],
        title:
          'Job Fairs System | CALABARZON'
      }
    };


    // ============================================================================
    // HTML RENDERER
    // ============================================================================
    function tryRenderHtmlOutput(
      fileNames,
      title,
      req = null
    ) {

      for (
        const fileName of fileNames
      ) {

        const candidateName =
          fileName.endsWith('.html')
            ? fileName
            : `${fileName}.html`;


        const filePath =
          path.join(
            VIEWS_DIR,
            candidateName
          );


        if (
          fs.existsSync(
            filePath
          )
        ) {

          let content =
            fs.readFileSync(
              filePath,
              'utf8'
            );


          let usernameParam =
            req &&
            req.query
              ? (
                  req.query.username ||
                  req.query.user ||
                  req.query.office ||
                  ''
                )
              : '';


          usernameParam =
            String(
              usernameParam
            ).trim();


          let injectScript =
            '';


          if (
            usernameParam &&
            usernameParam.toUpperCase() !==
              'ADMINISTRATOR'
          ) {

            const cleanUser =
              usernameParam
                .toUpperCase()
                .replace(
                  /"/g,
                  '\\"'
                );


            injectScript = `
    <script>
    (function() {
      try {
        var u = "${cleanUser}";

        sessionStorage.setItem(
          "username",
          u
        );

        sessionStorage.setItem(
          "userContext",
          JSON.stringify({
            username: u,
            name: u,
            role: u
          })
        );

        localStorage.setItem(
          "username",
          u
        );

        window.currentUser = u;

      } catch(e) {}
    })();
    </script>`;
          }


          if (
            content.includes(
              '<head>'
            )
          ) {

            content =
              content.replace(
                '<head>',
                `<head>${injectScript}<title>${title}</title><meta name="viewport" content="width=device-width, initial-scale=1">`
              );
          }


          return content;
        }
      }


      return null;
    }


    // ============================================================================
    // DO GET
    // ============================================================================
    function handleDoGet(
      req,
      res
    ) {

      const rawPage =
        req.query.page
          ? String(
              req.query.page
            ).trim()
          : 'index';


      const cleanPage =
        rawPage.toLowerCase();


      const target =
        PAGE_CONFIG[
          cleanPage
        ] || {
          files: [
            rawPage,
            'index'
          ],
          title:
            'Job Fair Report System | CALABARZON'
        };


      const output =
        tryRenderHtmlOutput(
          target.files,
          target.title,
          req
        );


      if (output) {
        return res.send(
          output
        );
      }


      return res.status(404).send(
        `<h3>Page Not Found: ${rawPage}</h3>`
      );
    }


    // ============================================================================
    // AUTH SESSION COOKIE HELPERS
    // ============================================================================
    const AUTH_SESSION_SECRET =
      process.env.AUTH_SESSION_SECRET ||
      'DOLE_CALABARZON_CHANGE_AUTH_SESSION_SECRET';

    function base64UrlEncode(value) {
      return Buffer.from(String(value), 'utf8')
        .toString('base64')
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');
    }

    function base64UrlDecode(value) {
      let text = String(value || '')
        .replace(/-/g, '+')
        .replace(/_/g, '/');
      while (text.length % 4) text += '=';
      return Buffer.from(text, 'base64').toString('utf8');
    }

    function createAuthCookieValue(username) {
      const encoded = base64UrlEncode(JSON.stringify({
        username: String(username || '').trim().toUpperCase(),
        issuedAt: Date.now()
      }));
      const signature = crypto.createHmac('sha256', AUTH_SESSION_SECRET)
        .update(encoded)
        .digest('base64')
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');
      return `${encoded}.${signature}`;
    }

    function readCookie(req, name) {
      const header = String(req.headers.cookie || '');
      for (const part of header.split(';')) {
        const index = part.indexOf('=');
        if (index === -1) continue;
        if (part.slice(0, index).trim() !== name) continue;
        return decodeURIComponent(part.slice(index + 1).trim());
      }
      return null;
    }

    function verifyAuthCookieValue(value) {
      if (!value || !value.includes('.')) return null;
      const dot = value.lastIndexOf('.');
      const encoded = value.slice(0, dot);
      const signature = value.slice(dot + 1);
      if (!encoded || !signature) return null;
      const expected = crypto.createHmac('sha256', AUTH_SESSION_SECRET)
        .update(encoded)
        .digest('base64')
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');
      const a = Buffer.from(signature);
      const b = Buffer.from(expected);
      if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
      try {
        const payload = JSON.parse(base64UrlDecode(encoded));
        if (!payload || !payload.username) return null;
        if (payload.issuedAt && Date.now() - Number(payload.issuedAt) > 8 * 60 * 60 * 1000) {
          return null;
        }
        return String(payload.username).trim().toUpperCase();
      } catch (err) {
        return null;
      }
    }

    function setAuthCookie(res, username) {
      const secure =
        process.env.NODE_ENV === 'production' ||
        process.env.COOKIE_SECURE === 'true';
      const sameSite =
        process.env.COOKIE_SAMESITE ||
        (secure ? 'None' : 'Lax');
      const parts = [
        `dole_auth=${encodeURIComponent(createAuthCookieValue(username))}`,
        'Path=/',
        'HttpOnly',
        `SameSite=${sameSite}`,
        'Max-Age=28800'
      ];
      if (secure) parts.push('Secure');
      res.setHeader('Set-Cookie', parts.join('; '));
    }

    function clearAuthCookie(res) {
      res.setHeader(
        'Set-Cookie',
        'dole_auth=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'
      );
    }

    // AUTHENTICATION & USER ENDPOINTS
    // ============================================================================

    // ---------- LOGIN ----------
    app.post(
      '/api/auth/login',
      async (req, res) => {

        try {

          const {
            username,
            password
          } = req.body || {};


          let result = {};


          try {

            result =
              await authenticateUser(
                username,
                password
              ) || {};

          } catch (e) {

            result = {
              success: false,
              message:
                'Authentication failed.'
            };
          }


          if (
            result.success === false
          ) {

            return res.json({

              success: false,

              message:
                result.message ||
                'Invalid username or password.'

            });
          }


          const cleanUser =
            String(
              username ||
              'DOLE4A'
            )
              .trim()
              .toUpperCase();


          const finalUsername =
            cleanUser ===
              'ADMINISTRATOR'
              ? 'DOLE4A'
              : cleanUser;


          const registeredEmail =
            await getUserEmail(
              finalUsername
            );


          const isFirstLogin =
            !registeredEmail;


          const userObj =
            result.user || {

              username:
                finalUsername,

              name:
                finalUsername,

              role:
                finalUsername
            };


          if (registeredEmail) {
            userObj.email =
              registeredEmail;
          }


          setAuthCookie(
            res,
            finalUsername
          );


          return res.json({

            success: true,

            message:
              result.message ||
              'Login successful',

            username:
              finalUsername,

            isFirstLogin,

            needsEmail:
              isFirstLogin,

            email:
              registeredEmail ||
              null,

            user:
              userObj

          });

        } catch (err) {

          console.error(
            'Login error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              'Server error during login.'

          });
        }
      }
    );


    // ---------- SETUP EMAIL ----------
    app.post(
      '/api/auth/setup-email',
      async (req, res) => {

        try {

          const {
            username,
            email
          } = req.body || {};


          const cleanUser =
            String(
              username || ''
            )
              .trim()
              .toUpperCase();


          const cleanEmail =
            String(
              email || ''
            )
              .trim()
              .toLowerCase();


          if (!cleanUser) {

            return res.status(400).json({

              success: false,

              message:
                'Username is required.'

            });
          }


          if (
            !cleanEmail ||
            !cleanEmail.includes('@')
          ) {

            return res.status(400).json({

              success: false,

              message:
                'A valid email address is required.'

            });
          }


          const existingEmail = await getUserEmail(cleanUser);

          if (existingEmail) {
            return res.json({
              success: true,
              message: 'Recovery email is already registered.',
              email: existingEmail,
              username: cleanUser,
              needsEmail: false,
              isFirstLogin: false
            });
          }

          const saveResult = await setUserEmail(cleanUser, cleanEmail);

          if (!saveResult || saveResult.success !== true) {
            return res.status(400).json({
              success: false,
              message: saveResult && saveResult.message
                ? saveResult.message
                : 'Failed to save recovery email.'
            });
          }


          return res.json({

            success: true,

            message:
              'Email saved successfully. You can now use Forgot Password to receive an OTP.',

            email:
              cleanEmail,

            username:
              cleanUser

          });

        } catch (err) {

          console.error(
            'Setup email error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              'Failed to save email.'

          });
        }
      }
    );


    // ---------- CHANGE PASSWORD ----------
    app.post(
      '/api/auth/change-password',
      async (req, res) => {

        try {

          const {
            username,
            currentPassword,
            newPassword,
            confirmPassword
          } = req.body || {};


          if (
            !username ||
            !currentPassword ||
            !newPassword
          ) {

            return res.status(400).json({

              success: false,

              message:
                'Username, current password, and new password are required.'

            });
          }


          if (
            confirmPassword &&
            newPassword !== confirmPassword
          ) {

            return res.status(400).json({

              success: false,

              message:
                'New password and confirmation do not match.'

            });
          }


          if (
            String(newPassword).length < 6
          ) {

            return res.status(400).json({

              success: false,

              message:
                'New password must be at least 6 characters.'

            });
          }


          const result =
            await changeUserPassword(
              username,
              currentPassword,
              newPassword
            );


          return res.json(
            result || {
              success: true,
              message:
                'Password changed successfully.'
            }
          );

        } catch (err) {

          console.error(
            'Change password error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              err.message ||
              'Failed to change password.'

          });
        }
      }
    );


    // ---------- FORGOT PASSWORD – SEND OTP ----------
    app.post(
      '/api/auth/forgot-password',
      async (req, res) => {

        try {

          const {
            username
          } = req.body || {};


          const cleanUser =
            String(
              username || ''
            )
              .trim()
              .toUpperCase();


          if (!cleanUser) {

            return res.status(400).json({

              success: false,

              message:
                'Username is required.'

            });
          }


          const registeredEmail =
            await getUserEmail(
              cleanUser
            );


          if (!registeredEmail) {

            return res.status(400).json({

              success: false,

              message:
                'No email is registered for this account. Please log in first and set up your email, or contact the administrator.'

            });
          }


          const otpCode =
            generateOtp();


          const token =
            storeOtp(
              cleanUser,
              otpCode
            );


          const mailResult =
            await sendOtpEmail(
              registeredEmail,
              otpCode,
              cleanUser
            );


          const response = {

            success: true,

            message:
              'OTP sent to your registered email.',

            maskedEmail:
              maskEmail(
                registeredEmail
              ),

            token

          };





          return res.json(
            response
          );

        } catch (err) {

          console.error(
            'Forgot password error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              err.message ||
              'Failed to send OTP.'

          });
        }
      }
    );


    // ---------- VERIFY OTP ----------
    app.post(
      '/api/auth/verify-reset-code',
      (req, res) => {

        try {

          const {
            username,
            code,
            token
          } = req.body || {};


          const cleanUser =
            String(
              username || ''
            )
              .trim()
              .toUpperCase();


          if (
            !cleanUser ||
            !code
          ) {

            return res.status(400).json({

              success: false,

              message:
                'Username and OTP code are required.'

            });
          }


          const result =
            verifyOtp(
              cleanUser,
              code
            );


          if (
            !result.success
          ) {

            return res.json({

              success: false,

              message:
                result.message

            });
          }


          return res.json({

            success: true,

            message:
              'OTP verified successfully.',

            token:
              result.token

          });

        } catch (err) {

          console.error(
            'Verify OTP error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              'Failed to verify OTP.'

          });
        }
      }
    );


    // ---------- RESET PASSWORD ----------
    app.post(
      '/api/auth/reset-password',
      async (req, res) => {

        try {

          const {
            username,
            token,
            newPassword,
            confirmPassword
          } = req.body || {};


          const cleanUser =
            String(
              username || ''
            )
              .trim()
              .toUpperCase();


          if (
            !cleanUser ||
            !newPassword
          ) {

            return res.status(400).json({

              success: false,

              message:
                'Username and new password are required.'

            });
          }


          if (
            confirmPassword &&
            newPassword !== confirmPassword
          ) {

            return res.status(400).json({

              success: false,

              message:
                'New password and confirmation do not match.'

            });
          }


          if (
            String(newPassword).length < 6
          ) {

            return res.status(400).json({

              success: false,

              message:
                'New password must be at least 6 characters.'

            });
          }


          if (token) {

            const valid =
              consumeResetToken(
                cleanUser,
                token
              );


            if (!valid) {

              return res.status(400).json({

                success: false,

                message:
                  'Invalid or expired reset token. Please request a new OTP.'

              });
            }

          } else {

            const entry =
              dbOtpStore[
                cleanUser
              ];


            if (
              !entry ||
              !entry.verified
            ) {

              return res.status(400).json({

                success: false,

                message:
                  'Please verify your OTP first.'

              });
            }


            delete dbOtpStore[
              cleanUser
            ];

            saveOtpStore();
          }


          const result = await setUserPassword(cleanUser, newPassword);


          if (
            result.success === false
          ) {

            return res.json({

              success: false,

              message:
                result.message ||
                'Failed to reset password.'

            });
          }


          return res.json({

            success: true,

            message:
              result.message ||
              'Password has been reset successfully. You can now sign in with your new password.'

          });

        } catch (err) {

          console.error(
            'Reset password error:',
            err
          );

          return res.status(500).json({

            success: false,

            message:
              err.message ||
              'Failed to reset password.'

          });
        }
      }
    );


    // ============================================================================
    // LEGACY SIMPLE RESET
    // ============================================================================
    app.post(
      '/api/auth/reset-password-simple',
      async (req, res) => {

        const {
          username
        } = req.body || {};


        try {

          return res.json(
            await resetUserPassword(
              username
            )
          );

        } catch (err) {

          return res.status(500).json({

            success: false,

            message:
              err.message ||
              'Failed to reset password.'

          });
        }
      }
    );


    // ============================================================================
    // AUTH SESSION / LOGOUT
    // ============================================================================
    // SPRS.html checks this endpoint before loading the report.
    // The login endpoint creates the signed cookie above.
    // ============================================================================
    app.get(
      '/api/auth/session',
      async (req, res) => {
        try {
          const username = verifyAuthCookieValue(
            readCookie(req, 'dole_auth')
          );

          if (!username) {
            return res.json({
              authenticated: false,
              user: null
            });
          }

          const userCtx =
            getCurrentUserContext(username) || {};

          return res.json({
            authenticated: true,
            username: userCtx.username || username,
            province:
              userCtx.province ||
              userCtx.region ||
              '',
            municipality:
              userCtx.municipality ||
              userCtx.cityMunicipality ||
              userCtx.office ||
              userCtx.fieldOffice ||
              username,
            user: {
              username: userCtx.username || username,
              name: userCtx.name || username,
              role: userCtx.role || 'user',
              province:
                userCtx.province ||
                userCtx.region ||
                '',
              municipality:
                userCtx.municipality ||
                userCtx.cityMunicipality ||
                userCtx.office ||
                userCtx.fieldOffice ||
                username,
              office:
                userCtx.office ||
                userCtx.municipality ||
                userCtx.cityMunicipality ||
                userCtx.fieldOffice ||
                username,
              fieldOffice:
                userCtx.fieldOffice ||
                userCtx.office ||
                userCtx.municipality ||
                userCtx.cityMunicipality ||
                username,
              email: await getUserEmail(username) || null
            }
          });
        } catch (err) {
          console.error('Auth session error:', err);
          return res.json({
            authenticated: false,
            user: null
          });
        }
      }
    );

    app.post(
      '/api/auth/logout',
      (req, res) => {
        try {
          clearAuthCookie(res);

          if (req.session && typeof req.session.destroy === 'function') {
            return req.session.destroy(() => {
              res.json({
                success: true,
                message: 'Logged out successfully.'
              });
            });
          }

          return res.json({
            success: true,
            message: 'Logged out successfully.'
          });
        } catch (err) {
          console.error('Logout error:', err);
          clearAuthCookie(res);
          return res.json({
            success: true,
            message: 'Logged out successfully.'
          });
        }
      }
    );

    // ============================================================================
    // USER CONTEXT
    // ============================================================================
    app.get(
      '/api/user/context',
      async (req, res) => {

        const requestedUser =
          (
            req.query.username ||
            req.query.user ||
            req.query.office ||
            ''
          ).trim();


        let userCtx =
          requestedUser
            ? getCurrentUserContext(
                requestedUser
              )
            : null;


        const defaultName =
          (
            requestedUser &&
            requestedUser.toUpperCase() !==
              'ADMINISTRATOR'
          )
            ? requestedUser.toUpperCase()
            : 'DOLE4A';


        if (
          !userCtx ||
          !userCtx.username
        ) {

          userCtx = {

            username:
              defaultName,

            name:
              defaultName,

            role:
              defaultName,

            office:
              defaultName,

            fieldOffice:
              defaultName
          };
        }


        const email =
          await getUserEmail(
            userCtx.username
          );


        if (email) {
          userCtx.email =
            email;
        }


        res.json({

          success: true,

          userContext:
            userCtx,

          ...userCtx

        });
      }
    );


    // ============================================================================
    // USER AUTHORIZED
    // ============================================================================
    app.get(
      '/api/user/authorized',
      (req, res) => {

        res.json({

          email:
            req.query.email,

          authorized:
            isUserAuthorized(
              req.query.email
            )

        });
      }
    );


    // ============================================================================
    // JOB FAIR PERMIT & CLEARANCE API ENDPOINTS
    // ============================================================================
    app.get(
      '/api/jf/records',
      (req, res) => {

        try {

          res.json({

            success: true,

            records:
              getJfRecords()

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    app.get(
      '/api/jf/summary',
      (req, res) => {

        try {

          res.json({

            success: true,

            records:
              getJfRecords()

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // JF ENCODE
    // ============================================================================
    app.post(
      '/api/jf/encode',
      (req, res) => {

        try {

          const data =
            req.body || {};


          let records =
            getJfRecords();


          const now =
            new Date();


          let proofReceivedUrl =
            data.proofReceived ||
            data.proofReceivedUrl ||
            PROOF_RECEIVED_DRIVE_URL;


          let proofReleasedUrl =
            data.proofReleased ||
            data.proofReleasedUrl ||
            PROOF_RELEASED_DRIVE_URL;


          if (
            proofReceivedUrl.startsWith(
              "data:"
            ) ||
            data.proofReceivedBase64
          ) {

            uploadAttachmentToDrive(
              data.proofReceivedBase64 ||
                proofReceivedUrl,

              data.proofReceivedName ||
                "proof_received",

              data.proofReceivedMime ||
                "",

              "RECEIVED"
            );


            proofReceivedUrl =
              PROOF_RECEIVED_DRIVE_URL;
          }


          if (
            proofReleasedUrl.startsWith(
              "data:"
            ) ||
            data.proofReleasedBase64
          ) {

            uploadAttachmentToDrive(
              data.proofReleasedBase64 ||
                proofReleasedUrl,

              data.proofReleasedName ||
                "proof_released",

              data.proofReleasedMime ||
                "",

              "RELEASED"
            );


            proofReleasedUrl =
              PROOF_RELEASED_DRIVE_URL;
          }


          const newRecord = {

            rowIndex:
              records.length > 0
                ? (
                    Math.max(
                      ...records.map(
                        r =>
                          Number(
                            r.rowIndex
                          ) || 0
                      )
                    ) + 1
                  )
                : 2,

            year:
              data.year ||
              now.getFullYear().toString(),

            fieldOffice:
              data.fieldOffice ||
              "",

            reportingPeriod:
              data.reportingPeriod ||
              "",

            sponsor:
              data.sponsor ||
              "",

            contactNumber:
              data.contactNumber ||
              "",

            documentNumber:
              data.documentNumber ||
              "",

            documentApplied:
              data.documentApplied ||
              "",

            jobFairVenue:
              data.jobFairVenue ||
              "",

            dateOfJobFair:
              data.dateOfJobFair ||
              "",

            dateFiled:
              data.dateFiled ||
              "",

            dateIssued:
              data.dateIssued ||
              "",

            dateOfEncoding:
              data.dateOfEncoding ||
              "",

            turnaroundTime:
              data.turnaroundTime ||
              "",

            daysFiledBeforeJF:
              data.daysFiledBeforeJF ||
              "",

            daysReported:
              data.daysReported ||
              "",

            actionTaken:
              data.actionTaken ||
              "PENDING",

            disapprovedReason:
              data.disapprovedReason ||
              "",

            proofReceived:
              proofReceivedUrl,

            proofReleased:
              proofReleasedUrl,

            userEmail:
              data.userEmail ||
              "",

            // Keep every field submitted by the existing JF Encoding page.
            ...data
          };


          newRecord.proofReceived =
            proofReceivedUrl;


          newRecord.proofReleased =
            proofReleasedUrl;


          newRecord.proofReceivedUrl =
            proofReceivedUrl;


          newRecord.proofReleasedUrl =
            proofReleasedUrl;


          delete newRecord.proofReceivedBase64;

          delete newRecord.proofReleasedBase64;


          const existingIdx =
            records.findIndex(
              r =>

                (
                  data.documentNumber &&
                  r.documentNumber ===
                    data.documentNumber
                )

                ||

                (
                  data.applicationId &&
                  r.applicationId ===
                    data.applicationId
                )

                ||

                (
                  data.rowIndex &&
                  r.rowIndex ==
                    data.rowIndex
                )
            );


          if (
            existingIdx !== -1
          ) {

            records[
              existingIdx
            ] = {

              ...records[
                existingIdx
              ],

              ...newRecord

            };

          } else {

            records.push(
              newRecord
            );
          }


          saveJfRecords(
            records
          );


          res.json({

            success: true,

            message:
              "Record details saved to JF Database successfully!"

          });

        } catch (err) {

          console.error(
            "JF encode error:",
            err
          );

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // UPDATE RECORD
    // ============================================================================
    app.post(
      '/api/jf/update-record',
      (req, res) => {

        try {

          const {
            rowIndex,
            ...updates
          } = req.body;


          let records =
            getJfRecords();


          const idx =
            records.findIndex(
              r =>
                r.rowIndex ==
                rowIndex
            );


          if (
            idx !== -1
          ) {

            records[idx] = {

              ...records[idx],

              ...updates

            };


            saveJfRecords(
              records
            );


            return res.json({

              success: true,

              message:
                "Record updated successfully!"

            });
          }


          res.status(404).json({

            success: false,

            message:
              "Record not found."

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // DELETE RECORD
    // ============================================================================
    app.post(
      '/api/jf/delete-record',
      (req, res) => {

        try {

          const {
            rowIndex
          } = req.body;


          let records =
            getJfRecords();


          const initialLen =
            records.length;


          records =
            records.filter(
              r =>
                r.rowIndex !=
                rowIndex
            );


          if (
            records.length <
            initialLen
          ) {

            saveJfRecords(
              records
            );


            return res.json({

              success: true,

              message:
                "Record deleted successfully!"

            });
          }


          res.status(404).json({

            success: false,

            message:
              "Record not found."

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // MARK AS DONE
    // ============================================================================
    app.post(
      '/api/jf/mark-as-done',
      (req, res) => {

        try {

          const payload =
            req.body || {};


          let records =
            getJfRecords();


          let idx =
            records.findIndex(
              r =>

                (
                  payload.rowIndex &&
                  r.rowIndex ==
                    payload.rowIndex
                )

                ||

                (
                  payload.documentNumber &&
                  r.documentNumber ===
                    payload.documentNumber
                )

                ||

                (
                  payload.applicationId &&
                  r.applicationId ===
                    payload.applicationId
                )
            );


          if (
            payload.proofReceivedBase64
          ) {

            uploadAttachmentToDrive(
              payload.proofReceivedBase64,

              payload.proofReceivedName ||
                "proof_received",

              payload.proofReceivedMime ||
                "",

              "RECEIVED"
            );
          }


          if (
            payload.proofReleasedBase64
          ) {

            uploadAttachmentToDrive(
              payload.proofReleasedBase64,

              payload.proofReleasedName ||
                "proof_released",

              payload.proofReleasedMime ||
                "",

              "RELEASED"
            );
          }


          const proofReceivedUrl =
            PROOF_RECEIVED_DRIVE_URL;


          const proofReleasedUrl =
            PROOF_RELEASED_DRIVE_URL;


          if (
            idx !== -1
          ) {

            records[idx] = {

              ...records[idx],

              ...payload,

              actionTaken:
                payload.actionTaken ||
                records[idx].actionTaken ||
                "APPROVED",

              disapprovedReason:
                payload.disapprovedReason ||
                records[idx].disapprovedReason ||
                "",

              proofReceived:
                proofReceivedUrl,

              proofReleased:
                proofReleasedUrl,

              proofReceivedUrl:
                proofReceivedUrl,

              proofReleasedUrl:
                proofReleasedUrl
            };


            delete records[idx]
              .proofReceivedBase64;


            delete records[idx]
              .proofReleasedBase64;

          } else {

            const newRecord = {

              rowIndex:
                records.length > 0
                  ? (
                      Math.max(
                        ...records.map(
                          r =>
                            Number(
                              r.rowIndex
                            ) || 0
                        )
                      ) + 1
                    )
                  : 2,

              ...payload,

              proofReceived:
                proofReceivedUrl,

              proofReleased:
                proofReleasedUrl,

              proofReceivedUrl:
                proofReceivedUrl,

              proofReleasedUrl:
                proofReleasedUrl
            };


            delete newRecord
              .proofReceivedBase64;


            delete newRecord
              .proofReleasedBase64;


            records.push(
              newRecord
            );
          }


          saveJfRecords(
            records
          );


          res.json({

            success: true,

            message:
              "Record details marked as done and attachments linked to Google Drive folders!",

            proofReceivedUrl,

            proofReleasedUrl

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // SEND RESULT EMAIL
    // ============================================================================
    app.post(
      '/api/jf/send-result-email',
      async (req, res) => {

        try {

          const payload =
            req.body || {};


          const toEmail =
            String(
              payload.to || ''
            ).trim();


          if (!toEmail) {

            return res.status(400).json({

              success: false,

              message:
                "RECIPIENT EMAIL ADDRESS IS MISSING. PLEASE CHECK THE USER EMAIL ADDRESS FIELD ON THE APPLICATION."

            });
          }


          const actionTaken =
            String(
              payload.actionTaken ||
              ''
            ).toUpperCase();


          const isApproved =
            actionTaken ===
            'APPROVED';


          const subject =
            `JOB FAIR ${
              payload.documentApplied ||
              'APPLICATION'
            } ${
              isApproved
                ? 'APPROVED'
                : 'DISAPPROVED'
            } - ${
              payload.documentNumber ||
              ''
            }`.trim();


          const bodyLines = [

            `Dear Applicant,`,

            ``,

            `This is to inform you that your Job Fair ${
              payload.documentApplied ||
              'application'
            } (${
              payload.documentNumber ||
              'N/A'
            }) submitted for ${
              payload.organizer ||
              'N/A'
            } (${
              payload.province ||
              'N/A'
            }) has been marked as ${
              actionTaken ||
              'PROCESSED'
            }.`,

            ``,

            `Date of Job Fair: ${
              payload.dateOfJobFair ||
              'N/A'
            }`,

            `Date Filed: ${
              payload.dateFiled ||
              'N/A'
            }`,

            `Date Issued: ${
              payload.dateIssued ||
              'N/A'
            }`,

            !isApproved
              ? `Reason for Disapproval: ${
                  payload.disapprovedReason ||
                  'N/A'
                }`
              : '',

            ``,

            `Official Google Drive Document Links:`,

            `Proof of Receipt: ${PROOF_RECEIVED_DRIVE_URL}`,

            `Proof of Release: ${PROOF_RELEASED_DRIVE_URL}`,

            ``,

            `Please find the attached document files for your reference.`,

            ``,

            `Thank you,`,

            `DOLE CALABARZON / PESO Job Fair Permit & Clearance System`

          ]
            .filter(Boolean)
            .join('\n');


          const attachments =
            (
              Array.isArray(
                payload.attachments
              )
                ? payload.attachments
                : []
            )
              .filter(
                a =>
                  a &&
                  (
                    a.data ||
                    a.url ||
                    a.path
                  )
              )
              .map(
                a => {

                  if (
                    a.data &&
                    String(
                      a.data
                    ).startsWith(
                      'data:'
                    )
                  ) {

                    const dataUrl =
                      String(
                        a.data || ''
                      );


                    const commaIdx =
                      dataUrl.indexOf(
                        ','
                      );


                    const base64Data =
                      commaIdx !== -1
                        ? dataUrl.slice(
                            commaIdx + 1
                          )
                        : dataUrl;


                    return {

                      filename:
                        a.filename ||
                        'attachment',

                      content:
                        Buffer.from(
                          base64Data,
                          'base64'
                        ),

                      contentType:
                        a.contentType ||
                        'application/octet-stream'

                    };

                  } else if (
                    a.url ||
                    a.path
                  ) {

                    const targetUrl =
                      a.url ||
                      a.path;


                    if (
                      targetUrl.startsWith(
                        '/uploads/'
                      )
                    ) {

                      const filePath =
                        path.join(
                          __dirname,
                          'public',
                          targetUrl.replace(
                            /^\//,
                            ''
                          )
                        );


                      if (
                        fs.existsSync(
                          filePath
                        )
                      ) {

                        return {

                          filename:
                            a.filename ||
                            path.basename(
                              filePath
                            ),

                          path:
                            filePath

                        };
                      }
                    }


                    return null;
                  }

                  return null;
                }
              )
              .filter(Boolean);


          const transporter =
            getMailTransporter();


          if (transporter) {

            await transporter.sendMail({

              from:
                process.env.EMAIL_FROM ||
                process.env.EMAIL_USER,

              to:
                toEmail,

              subject,

              text:
                bodyLines,

              attachments

            });


            return res.json({

              success: true,

              message:
                "EMAIL SENT SUCCESSFULLY."

            });
          }


          console.warn(
            `[EMAIL NOT CONFIGURED] Would have sent to ${toEmail}: "${subject}". Set EMAIL_HOST, EMAIL_USER, EMAIL_PASS to enable real delivery.`
          );


          return res.json({

            success: true,

            simulated: true,

            message:
              "EMAIL SIMULATED (SMTP NOT CONFIGURED ON SERVER). RECORD WILL STILL BE FINALIZED."

          });

        } catch (err) {

          console.error(
            "Error in /api/jf/send-result-email:",
            err.message
          );


          return res.status(500).json({

            success: false,

            message:
              err.message ||
              "FAILED TO SEND EMAIL."

          });
        }
      }
    );


    // ============================================================================
    // TAB 4 BREAKDOWN
    // ============================================================================
    app.get(
      '/api/jf/tab4-breakdown',
      (req, res) => {

        try {

          const records =
            getJfRecords();


          const clearanceRecords =
            [];


          const permitRecords =
            [];


          records.forEach(
            r => {

              const doc =
                String(
                  r.documentApplied ||
                  ""
                ).toUpperCase();


              const item = {

                rowIndex:
                  r.rowIndex,

                month:
                  r.reportingPeriod ||
                  r.month ||
                  "JANUARY",

                year:
                  r.year ||
                  "2026",

                province:
                  r.fieldOffice ||
                  r.province ||
                  "BATANGAS",

                actionTaken:
                  r.actionTaken ||
                  "APPROVED",

                clearanceNo:
                  r.documentNumber ||
                  "",

                documentNumber:
                  r.documentNumber ||
                  "",

                proofReceived:
                  r.proofReceived ||
                  PROOF_RECEIVED_DRIVE_URL,

                proofReleased:
                  r.proofReleased ||
                  PROOF_RELEASED_DRIVE_URL
              };


              if (
                doc.includes(
                  "PERMIT"
                )
              ) {

                permitRecords.push(
                  item
                );

              } else {

                clearanceRecords.push(
                  item
                );
              }

            }
          );


          res.json({

            success: true,

            clearanceRecords,

            permitRecords

          });

        } catch (err) {

          res.status(500).json({

            success: false,

            message:
              err.message

          });
        }
      }
    );


    // ============================================================================
    // GIP CUR CODE SOURCE - GOOGLE SHEET "WORK PROGRAM", COLUMN O
    // ============================================================================
    const GIP_CUR_SPREADSHEET_ID =
      '1XmIhBNFvjOSAMT1abEb6Xpm9M3QumJkT564VpzidP-0';

    const GIP_CUR_SHEET_NAME = 'ADL NO';

    function parseGoogleVisualizationResponse(text) {
      const raw = String(text || '').trim();

      // Google Visualization normally returns:
      // google.visualization.Query.setResponse({...});
      const start = raw.indexOf('{');
      const end = raw.lastIndexOf('}');

      if (start === -1 || end === -1 || end < start) {
        throw new Error('Google Sheet returned an invalid response.');
      }

      return JSON.parse(raw.slice(start, end + 1));
    }

    async function getCurCodeData() {
      // ========================================================================
      // GIP CUR / WORK PROGRAM SOURCE
      //
      // Source sheet: WORK PROGRAM
      // CUR NO.      = Column G
      // WORK PROGRAM = Column I
      //
      // The CUR dropdown and Work Program MUST be paired from the SAME ROW.
      // ========================================================================

      const url =
        'https://docs.google.com/spreadsheets/d/' +
        encodeURIComponent(GIP_CUR_SPREADSHEET_ID) +
        '/gviz/tq?tqx=out:json&headers=0&sheet=' +
        encodeURIComponent(GIP_CUR_SHEET_NAME) +
        '&range=' +
        encodeURIComponent('G:I');

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'text/plain,application/json'
        },
        redirect: 'follow'
      });

      const text = await response.text();

      if (!response.ok) {
        throw new Error(
          'Unable to read WORK PROGRAM columns G:I. Google returned HTTP ' +
          response.status + '.'
        );
      }

      const payload = parseGoogleVisualizationResponse(text);

      if (payload && payload.status === 'error') {
        const message =
          payload.errors &&
          payload.errors[0] &&
          payload.errors[0].detailed_message
            ? payload.errors[0].detailed_message
            : 'Google Sheet query failed.';

        throw new Error(message);
      }

      const rows =
        payload &&
        payload.table &&
        Array.isArray(payload.table.rows)
          ? payload.table.rows
          : [];

      // Because the requested range is G:I:
      // index 0 = Column G = CUR NO.
      // index 1 = Column H
      // index 2 = Column I = WORK PROGRAM
      const CUR_INDEX = 0;
      const WORK_PROGRAM_INDEX = 2;

      const cellText = cell => {
        if (!cell) return '';

        // Keep the exact displayed value when Google provides one.
        if (cell.f !== undefined && cell.f !== null) {
          return String(cell.f).trim();
        }

        if (cell.v !== undefined && cell.v !== null) {
          return String(cell.v).trim();
        }

        return '';
      };

      const curCodes = [];
      const curMap = {};

      rows.forEach(row => {
        const cells =
          row && Array.isArray(row.c)
            ? row.c
            : [];

        const curNo =
          cellText(cells[CUR_INDEX]);

        const workProgram =
          cellText(cells[WORK_PROGRAM_INDEX]);

        if (!curNo) return;

        // Skip common header text if the first row contains headers.
        const normalizedCur =
          curNo
            .toUpperCase()
            .replace(/\s+/g, ' ')
            .trim();

        if (
          normalizedCur === 'CUR' ||
          normalizedCur === 'CUR NO' ||
          normalizedCur === 'CUR NO.' ||
          normalizedCur === 'CUR CODE' ||
          normalizedCur === 'CUR NUMBER'
        ) {
          return;
        }

        // CUR dropdown values come directly from Column G.
        if (!curCodes.includes(curNo)) {
          curCodes.push(curNo);
        }

        // Work Program comes directly from Column I of the SAME ROW.
        // Keep the first non-empty mapping if duplicate CUR numbers exist.
        if (!(curNo in curMap) || (!curMap[curNo] && workProgram)) {
          curMap[curNo] = workProgram;
        }
      });

      console.log(
        'Loaded CUR/Work Program mapping from WORK PROGRAM!G:I:',
        curCodes.length,
        'CUR numbers'
      );

      return {
        success: true,
        curCodes,
        codes: curCodes,
        curCodeList: curCodes,
        curMap,
        curCodeMap: curMap,
        map: curMap
      };
    }




    // ============================================================================
    // GIP AUTO-FILL MASTER LIST SOURCE
    // Google Sheet: FORM RESPONSES 1
    // Full Name = Column AB (row 2 onward)
    // Credential Type = Column O; Training Title = Column AA.
    // Month is resolved from its header, not assumed to be Column AB.
    // ============================================================================
    const GIP_AUTOFILL_SPREADSHEET_ID =
      '1dzIgLOFRfb2ywym92nCUGKJY1EGWcj8r8yJ2WyG0A_M';

    const GIP_AUTOFILL_SHEET_NAME = 'FORM RESPONSES 1';
    const GIP_AUTOFILL_SHEET_GID = '508419630'; // Exact source tab from supplied Google Sheets link.

    function normalizeGipAutoFillMonth(value) {
      const text = String(value == null ? '' : value).trim().toUpperCase();
      if (!text) return '';

      const months = {
        JAN: 'JANUARY', JANUARY: 'JANUARY',
        FEB: 'FEBRUARY', FEBRUARY: 'FEBRUARY',
        MAR: 'MARCH', MARCH: 'MARCH',
        APR: 'APRIL', APRIL: 'APRIL',
        MAY: 'MAY',
        JUN: 'JUNE', JUNE: 'JUNE',
        JUL: 'JULY', JULY: 'JULY',
        AUG: 'AUGUST', AUGUST: 'AUGUST',
        SEP: 'SEPTEMBER', SEPT: 'SEPTEMBER', SEPTEMBER: 'SEPTEMBER',
        OCT: 'OCTOBER', OCTOBER: 'OCTOBER',
        NOV: 'NOVEMBER', NOVEMBER: 'NOVEMBER',
        DEC: 'DECEMBER', DECEMBER: 'DECEMBER'
      };

      if (months[text]) return months[text];

      // Also accept a date value in Column AB if the sheet happens to contain dates.
      const d = new Date(value);
      if (!isNaN(d.getTime())) {
        return d.toLocaleString('en-US', { month: 'long' }).toUpperCase();
      }

      return text;
    }

    function gipGvizCellText(cell) {
      if (!cell) return '';
      if (cell.f !== undefined && cell.f !== null) return String(cell.f).trim();
      if (cell.v !== undefined && cell.v !== null) return String(cell.v).trim();
      return '';
    }

    function normalizeGipHeader(value) {
      return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[\r\n]+/g, ' ')
        .replace(/\s*\/\s*/g, '/')
        .replace(/\s+/g, ' ')
        .replace(/[:?]+$/g, '');
    }

    function firstGipHeaderValue(record, names) {
      for (const name of names) {
        const key = normalizeGipHeader(name);
        if (record[key] !== undefined && record[key] !== null && String(record[key]).trim() !== '') {
          return String(record[key]).trim();
        }
      }
      return '';
    }

    async function getMasterListForAutoFill(selectedMonth) {
      const wantedMonth = ''; // Search all names regardless of selected encoding month.

      // A:AB is loaded so Column O (14), AA (26), and AB (27)
      // are available from the same applicant row.
      const url =
        'https://docs.google.com/spreadsheets/d/' +
        encodeURIComponent(GIP_AUTOFILL_SPREADSHEET_ID) +
        '/gviz/tq?tqx=out:json&headers=0&gid=' +
        encodeURIComponent(GIP_AUTOFILL_SHEET_GID) +
        '&range=' + encodeURIComponent('A:AB');

      const response = await fetch(url, {
        method: 'GET',
        headers: { 'Accept': 'text/plain,application/json' },
        redirect: 'follow'
      });

      const text = await response.text();

      if (!response.ok) {
        throw new Error(
          'Unable to read GIP auto-fill source FORM RESPONSES 1!A:AB. Google returned HTTP ' +
          response.status + '.'
        );
      }

      const payload = parseGoogleVisualizationResponse(text);

      if (payload && payload.status === 'error') {
        const message =
          payload.errors && payload.errors[0] && payload.errors[0].detailed_message
            ? payload.errors[0].detailed_message
            : 'Google Sheet auto-fill query failed.';
        throw new Error(message);
      }

      const rows = payload && payload.table && Array.isArray(payload.table.rows)
        ? payload.table.rows
        : [];

      if (!rows.length) return [];

      // Row 1 contains headers. Data begins at row 2 as requested.
      const headerCells = Array.isArray(rows[0].c) ? rows[0].c : [];
      const headers = [];
      for (let i = 0; i < 28; i++) {
        headers[i] = normalizeGipHeader(gipGvizCellText(headerCells[i]) || ('COLUMN_' + (i + 1)));
      }

      const FULL_NAME_INDEX = 27; // AB
      const CREDENTIAL_INDEX = 14; // O
      const TRAINING_INDEX = 26; // AA
      const MONTH_INDEX = headers.findIndex(h => ['MONTH', 'REPORTING MONTH', 'ENTRY MONTH'].includes(h));
      const result = [];

      for (let r = 1; r < rows.length; r++) {
        const cells = rows[r] && Array.isArray(rows[r].c) ? rows[r].c : [];
        const fullName = gipGvizCellText(cells[FULL_NAME_INDEX]).trim();
        const rowMonthRaw = MONTH_INDEX >= 0 ? gipGvizCellText(cells[MONTH_INDEX]).trim() : '';
        const rowMonth = normalizeGipAutoFillMonth(rowMonthRaw);

        if (!fullName) continue;
        // Do not restrict name suggestions by the Encoding tab month.

        const byHeader = {};
        for (let c = 0; c < 28; c++) {
          const value = gipGvizCellText(cells[c]);
          const header = headers[c];
          if (header && byHeader[header] === undefined) byHeader[header] = value;
        }

        // Return both the original header-based values and the field aliases already
        // understood by the existing GIP.html autoFillFromFullNameSearch() function.
        const record = {
          ...byHeader,
          fullName,
          'FULL NAME': fullName,
          month: rowMonthRaw,
          'MONTH': rowMonthRaw,
          sourceRow: r + 1,
          rawRow: cells.map(gipGvizCellText),

          lastName: firstGipHeaderValue(byHeader, ['LAST NAME', 'LAST NAME:', 'SURNAME']),
          firstName: firstGipHeaderValue(byHeader, ['FIRST NAME', 'FIRST NAME:', 'GIVEN NAME']),
          middleName: firstGipHeaderValue(byHeader, ['MIDDLE NAME', 'MIDDLE NAME:', 'MIDDLE INITIAL']),
          suffix: firstGipHeaderValue(byHeader, ['SUFFIX', 'SUFFIX:', 'EXTENSION NAME', 'NAME EXTENSION']),
          address: firstGipHeaderValue(byHeader, ['ADDRESS', 'ADDRESS:', 'COMPLETE ADDRESS', 'HOME ADDRESS']),
          // DATE OF BIRTH is explicitly stored in source Google Sheet COLUMN G.
          // Array index 6 = Column G. Do not depend on the row-1 header name.
          dob: gipGvizCellText(cells[6]) || firstGipHeaderValue(byHeader, ['DATE OF BIRTH', 'DATE OF BIRTH:', 'BIRTHDATE', 'BIRTH DATE']),
          dateOfBirth: gipGvizCellText(cells[6]) || firstGipHeaderValue(byHeader, ['DATE OF BIRTH', 'DATE OF BIRTH:', 'BIRTHDATE', 'BIRTH DATE']),
          birthDate: gipGvizCellText(cells[6]) || firstGipHeaderValue(byHeader, ['DATE OF BIRTH', 'DATE OF BIRTH:', 'BIRTHDATE', 'BIRTH DATE']),
          'DATE OF BIRTH': gipGvizCellText(cells[6]) || firstGipHeaderValue(byHeader, ['DATE OF BIRTH', 'DATE OF BIRTH:', 'BIRTHDATE', 'BIRTH DATE']),
          age: firstGipHeaderValue(byHeader, ['AGE', 'AGE:']),
          gender: firstGipHeaderValue(byHeader, ['GENDER', 'GENDER:', 'SEX', 'SEX:']),
          education: firstGipHeaderValue(byHeader, ['EDUCATIONAL ATTAINMENT', 'EDUCATIONAL ATTAINMENT:']),
          pwd: firstGipHeaderValue(byHeader, ['DISADVANTAGED GROUP', 'DISADVANTAGE GROUP', 'DISADVANTAGE GROUP:']),
          // Exact physical columns supplied by the user; avoid header mismatch.
          credentialType: gipGvizCellText(cells[CREDENTIAL_INDEX]).trim(), // O
          trainingTitle: gipGvizCellText(cells[TRAINING_INDEX]).trim(), // AA
          emailAddress: firstGipHeaderValue(byHeader, ['EMAIL ADDRESS', 'EMAIL ADDRESS:', 'EMAIL']),
          contactNumber: (() => {
            // K is zero-based index 10. Prefer displayed text to preserve
            // leading zeros; fall back to the raw numeric value if necessary.
            const cell = cells[10];
            let number = gipGvizCellText(cell) || firstGipHeaderValue(byHeader,
              ['CONTACT NUMBER', 'CONTACT NUMBER:', 'MOBILE NUMBER', 'CELLPHONE NUMBER']);
            number = String(number || '').trim();
            if (/^9\d{9}$/.test(number)) number = '0' + number;
            else if (/^63(9\d{9})$/.test(number)) number = '0' + number.slice(2);
            return number;
          })(), // Column K
          idType: firstGipHeaderValue(byHeader, ['TYPE OF ID', 'TYPE OF ID:', 'ID TYPE']),
          idNumber: gipGvizCellText(cells[24]) || firstGipHeaderValue(byHeader, ['ID NUMBER', 'ID NUMBER:', 'ID NO.', 'ID NO']), // Column Y
          datePlaceOfIssue: firstGipHeaderValue(byHeader, ['DATE/PLACE OF ISSUE', 'DATE/PLACE OF ISSUE:', 'DATE / PLACE OF ISSUE', 'DATE AND PLACE OF ISSUE'])
        };

        result.push(record);
      }

      // Google Visualization may return null for telephone numbers when a
      // column mixes text and numeric cell types. CSV export preserves those
      // original displayed values. Recover Column K by physical sheet row.
      if (result.some(record => !record.contactNumber || !record.credentialType || !record.trainingTitle)) {
        try {
          const csvUrl = 'https://docs.google.com/spreadsheets/d/' +
            encodeURIComponent(GIP_AUTOFILL_SPREADSHEET_ID) +
            '/export?format=csv&gid=508419630';
          const csvResponse = await fetch(csvUrl, { redirect: 'follow' });
          if (csvResponse.ok) {
            const csvText = await csvResponse.text();
            if (!/^\s*</.test(csvText)) {
              const csvRows = [];
              let row = [], field = '', quoted = false;
              for (let i = 0; i < csvText.length; i++) {
                const ch = csvText[i];
                if (ch === '"') {
                  if (quoted && csvText[i + 1] === '"') { field += '"'; i++; }
                  else quoted = !quoted;
                } else if (ch === ',' && !quoted) {
                  row.push(field); field = '';
                } else if ((ch === '\n' || ch === '\r') && !quoted) {
                  if (ch === '\r' && csvText[i + 1] === '\n') i++;
                  row.push(field); csvRows.push(row); row = []; field = '';
                } else field += ch;
              }
              if (field || row.length) { row.push(field); csvRows.push(row); }
              for (const record of result) {
                if (record.contactNumber && record.credentialType && record.trainingTitle) continue;
                const source = csvRows[record.sourceRow - 1];
                // Confirm AB matches before using fallback columns to prevent
                // misalignment if the source has unexpected row ordering.
                if (!source || String(source[27] || '').trim().toUpperCase() !==
                    String(record.fullName || '').trim().toUpperCase()) continue;
                // Physical source columns: O = index 14, AA = index 26.
                if (!record.credentialType) record.credentialType = String(source[14] || '').trim();
                if (!record.trainingTitle) record.trainingTitle = String(source[26] || '').trim();
                // Keep rawRow consistent with the values returned to the HTML.
                if (record.credentialType) record.rawRow[14] = record.credentialType;
                if (record.trainingTitle) record.rawRow[26] = record.trainingTitle;
                if (record.contactNumber) continue;
                let phone = String(source[10] || '').trim();
                if (/^9\d{9}$/.test(phone)) phone = '0' + phone;
                else if (/^63(9\d{9})$/.test(phone)) phone = '0' + phone.slice(2);
                if (phone) { record.contactNumber = phone; record.rawRow[10] = phone; }
              }
            }
          } else {
            console.warn('GIP Column K CSV fallback returned HTTP', csvResponse.status);
          }
        } catch (csvError) {
          console.warn('GIP Column K CSV fallback failed:', csvError.message);
        }
      }

      console.log(
        'Loaded GIP auto-fill names from FORM RESPONSES 1 for month',
        wantedMonth || '(ALL)',
        ':',
        result.length,
        'record(s)'
      );

      return result;
    }




    // ============================================================================
    // GIP SUBMIT ENTRY - PROVINCE / APPLICATION TYPE GOOGLE SHEET ROUTING
    // ============================================================================
    // Each province/location has its own spreadsheet. The destination tab is NEW
    // or RENEW based on the GIP Application Type selected in GIP.html.
    const GIP_ENTRY_SPREADSHEET_IDS = {
      'BATANGAS': '10tGombCvvZdr1hBKg-nfBfelxDdbLX_pz3AzAsC9zSk',
      'CAVITE': '15YA0AiBXiEnx9gHbVrsoc6XtoAvaIui1XOitvLqWNtw',
      'LAGUNA': '1JV4-iqJWOSjSuRUn6i62n0oqRbLjVdD6LP_c4V3DcgA',
      'QUEZON': '1RQP_BMoC3zinX5v0ItdY-lNiIUegdj63717nTva2BhU',
      'RIZAL': '1p6p_BanbIOmwQ0G26VNdfYzLS4U0tfRgIeguB5Z3Tto',
      'REGIONAL OFFICE': '1jK443TDDk_k-XO3hSU0jBI4eL2WhA5S4mP5130pL2Oo'
    };

    // Deploy the companion Code.gs as a Web App (Execute as: Me; access as
    // appropriate for your deployment) and place its /exec URL in this environment
    // variable. Keeping the URL outside the HTML prevents exposing backend details.
    const GIP_SAVE_APPS_SCRIPT_URL =
      String(process.env.GIP_SAVE_APPS_SCRIPT_URL || 'https://script.google.com/macros/s/AKfycby_rTMEltubbtcu_aWUYMtR4cVD0h_tOHl9WympL7jZGhPrwNdSs98DCLcgPqZ5ZNM/exec').trim();

    function normalizeGipProvince(value) {
      const v = String(value || '').trim().toUpperCase().replace(/\s+/g, ' ');
      if (v === 'REGIONAL' || v === 'RO' || v === 'REGIONAL OFFICE') return 'REGIONAL OFFICE';
      return v;
    }

    function normalizeGipApplicationType(value) {
      const v = String(value || '').trim().toUpperCase();
      if (v === 'NEW') return 'NEW';
      if (v === 'RENEW' || v === 'RENEWAL') return 'RENEW';
      return '';
    }

    async function processGipForm(formData) {
      const data = formData && typeof formData === 'object' ? { ...formData } : {};
      const province = normalizeGipProvince(data.province);
      const sheetName = normalizeGipApplicationType(data.appType || data.applicationType);

      if (!province) {
        return { success: false, message: 'Province / Location is required.' };
      }

      const spreadsheetId = GIP_ENTRY_SPREADSHEET_IDS[province];
      if (!spreadsheetId) {
        return { success: false, message: 'No destination spreadsheet is configured for ' + province + '.' };
      }

      if (!sheetName) {
        return { success: false, message: 'Application Type must be NEW or RENEW.' };
      }

      if (!GIP_SAVE_APPS_SCRIPT_URL || !/^https:\/\/script\.google\.com\/macros\/s\/[^/]+\/exec(?:\?.*)?$/.test(GIP_SAVE_APPS_SCRIPT_URL)) {
        throw new Error(
          'GIP save is not configured: set GIP_SAVE_APPS_SCRIPT_URL to the deployed GIP Code.gs Web App /exec URL (not the authentication Apps Script URL).'
        );
      }

      // Proper-case only free-text name/address/description fields. Preserve email,
      // identifiers, abbreviations, categorical values and routing fields.
      const properCaseGip = value => String(value == null ? '' : value)
        .trim().replace(/\s+/g, ' ')
        .toLowerCase().replace(/(^|[\s\-'/])([a-zà-ÿ])/g, (m, prefix, letter) => prefix + letter.toUpperCase());
      const properCaseFields = [
        'firstName', 'middleName', 'lastName', 'address', 'trainingTitle',
        'natureOfWork', 'placeOfAssignmentOther', 'govtAgencyOther'
      ];
      for (const field of properCaseFields) {
        if (typeof data[field] === 'string' && data[field].trim()) {
          data[field] = properCaseGip(data[field]);
        }
      }
      // For custom assignments/agencies, submit the same properly-cased value
      // through the primary field expected by the spreadsheet integration.
      if (data.placeOfAssignmentOther && data.placeOfAssignment &&
          String(data.placeOfAssignment).trim().toUpperCase() === 'OTHERS') {
        data.placeOfAssignment = data.placeOfAssignmentOther;
      }
      if (data.govtAgencyOther && data.govtAgency &&
          String(data.govtAgency).trim().toUpperCase() === 'OTHERS') {
        data.govtAgency = data.govtAgencyOther;
      }
      // GIP destination columns: U = Government Agencies; V = Office / Place of Assignment.
      // ID fields are preserved and forwarded to Apps Script for AG, AH and AI.
      data.columnU = data.govtAgency || '';
      data.columnV = data.placeOfAssignment || '';
      data.columnAG = data.idType || '';
      data.columnAH = data.idNumber || '';
      data.columnAI = data.datePlaceOfIssue || '';

      // Force the validated routing values so the browser cannot choose an arbitrary
      // spreadsheet ID or sheet name.
      data.province = province;
      data.location = province;
      data.appType = sheetName === 'NEW' ? 'New' : 'Renew';

      // Preserve YEAR from the Encoding form all the way to Apps Script.
      // Column AF is the 32nd destination column.
      const submittedYear = parseInt(data.entryYear || data.year, 10);
      data.entryYear =
        Number.isInteger(submittedYear) && submittedYear >= 1900 && submittedYear <= 2100
          ? String(submittedYear)
          : String(new Date().getFullYear());
      data.year = data.entryYear;

      const response = await fetch(GIP_SAVE_APPS_SCRIPT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'saveGipEntry',
          spreadsheetId,
          sheetName,
          province,
          location: province,
          entryYear: data.entryYear,
          year: data.entryYear,
          formData: data
        }),
        redirect: 'follow'
      });

      const raw = await response.text();
      if (!response.ok) {
        throw new Error('GIP Google Sheet save failed with HTTP ' + response.status + ': ' + raw);
      }

      let result;
      try {
        result = JSON.parse(raw);
      } catch (e) {
        throw new Error('GIP save Web App returned invalid JSON.');
      }

      // A wrong Apps Script deployment can return a JSON "unknown action" error.
      // Report the actual configuration issue rather than suggesting the form is invalid.
      const upstreamMessage = String(result && (result.message || result.error || result.__error) || '');
      if (/unknown\s+(?:post\s+|api\s+)?action\s*:?\s*savegipentry|unsupported\s+action\s*:?\s*savegipentry/i.test(upstreamMessage)) {
        throw new Error(
          'The configured GIP Apps Script does not implement saveGipEntry. Deploy the GIP-specific Code.gs containing the saveGipEntry action, then update GIP_SAVE_APPS_SCRIPT_URL to that deployment /exec URL. No entry was saved.'
        );
      }

      // Reject an OLD Apps Script deployment. This prevents a stale /exec URL
      // from reporting success while leaving Column AF blank.
      if (result && result.success === true && result.apiVersion !== 'GIP-AF-20260922-1') {
        throw new Error(
          'GIP Apps Script deployment is outdated. Redeploy the updated Code.gs as a NEW VERSION and make sure GIP_SAVE_APPS_SCRIPT_URL points to that /exec deployment.'
        );
      }

      if (result && result.success === true) {
        const returnedYear = String(result.entryYear || '').trim();
        const returnedAf = String(result.columnAF || '').trim();
        if (!returnedYear || returnedAf !== returnedYear) {
          throw new Error(
            'GIP save verification failed: Column AF did not return the submitted YEAR.'
          );
        }
      }

      // IMPORTANT: preserve duplicate / eligibility information returned by Code.gs.
      // The browser needs requiresRenewalConfirmation, existingWorkMonths, etc.
      if (!result || result.success !== true) {
        return {
          ...(result && typeof result === 'object' ? result : {}),
          success: false,
          message: result && result.message ? result.message : 'Unable to save GIP entry.'
        };
      }

      return {
        ...(result && typeof result === 'object' ? result : {}),
        success: true,
        message: result.message || ('Entry saved to ' + province + ' / ' + sheetName + '.'),
        province,
        sheetName: result.sheetName || sheetName,
        row: result.row || null
      };
    }



    // ============================================================================
    // GIP SUMMARY - CONSOLIDATE ALL PROVINCES + REGIONAL OFFICE
    // Reads BOTH NEW and RENEW tabs from every configured destination spreadsheet.
    // Destination columns are fixed A:AF. Column AF stores the Encoding YEAR.
    // ============================================================================
    async function getGipSummaryData() {
      const allRecords = [];
      const errors = [];

      async function readOneTab(configuredProvince, spreadsheetId, sheetName) {
        // IMPORTANT:
        // Read ALL columns A:AF beginning at the actual Google Sheet row 15.
        // Because the range itself begins at row 15, rows[0] = Google Sheet row 15.
        const url =
          'https://docs.google.com/spreadsheets/d/' +
          encodeURIComponent(spreadsheetId) +
          '/gviz/tq?tqx=out:json&headers=0&sheet=' +
          encodeURIComponent(sheetName) +
          '&range=' + encodeURIComponent('A15:AF') +
          '&_=' + Date.now();

        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'Accept': 'text/plain,application/json',
            'Cache-Control': 'no-cache'
          },
          redirect: 'follow'
        });

        const text = await response.text();

        if (!response.ok) {
          throw new Error(
            configuredProvince + ' / ' + sheetName +
            ': Google returned HTTP ' + response.status + '.'
          );
        }

        const payload = parseGoogleVisualizationResponse(text);

        if (payload && payload.status === 'error') {
          const msg =
            payload.errors &&
            payload.errors[0] &&
            (payload.errors[0].detailed_message || payload.errors[0].message)
              ? (payload.errors[0].detailed_message || payload.errors[0].message)
              : 'Google Sheet query failed.';

          throw new Error(
            configuredProvince + ' / ' + sheetName + ': ' + msg
          );
        }

        const rows =
          payload &&
          payload.table &&
          Array.isArray(payload.table.rows)
            ? payload.table.rows
            : [];

        rows.forEach((row, index) => {
          const cells =
            row && Array.isArray(row.c)
              ? row.c
              : [];

          const v = i => gipGvizCellText(cells[i]);

          // Keep every actual data row starting at source row 15.
          // Ignore only rows where ALL A:AF cells are blank.
          let hasAnyData = false;
          for (let c = 0; c < 32; c++) {
            if (String(v(c) || '').trim() !== '') {
              hasAnyData = true;
              break;
            }
          }

          if (!hasAnyData) return;

          const sourceRow = 15 + index;

          // Exact source-column mapping requested for the Summary tab:
          // A  = Month
          // B  = Work Program
          // C  = CUR Code
          // D  = Last Name
          // E  = First Name
          // F  = Middle Name
          // G  = Suffix / Extension
          // H  = Full Name
          // I  = Address
          // J  = Date of Birth
          // K  = Age
          // L  = Gender
          // M  = Educational Attainment
          // N  = Disadvantaged Group
          // O  = ARAL Program
          // P  = Credential Type
          // Q  = Training Title
          // R  = Email Address
          // S  = Contact Number
          // T  = Documents Submitted
          // U  = Office / Place of Assignment
          // V  = Government Agencies
          // W  = Nature of Work
          // X  = Start Date
          // Y  = End Date
          // Z  = Work Months
          // AA = Remarks
          // AB = Replacement Of
          // AC = Province
          const startDate = v(23);
          const endDate = v(24);

          // YEAR FILTER SOURCE: Column AF only (zero-based index 31).
          // Do not derive the Summary year from Start Date, End Date, Month, or any other column.
          const afYearMatch = String(v(31) || '').match(/\b(19|20)\d{2}\b/);
          const savedYear = afYearMatch ? afYearMatch[0] : '';

          allRecords.push({
            sourceSpreadsheetId: spreadsheetId,
            sourceSheet: sheetName,
            sourceRow: sourceRow,
            sheetRowIndex: sourceRow,
            no: sourceRow,

            month: v(0),
            entryMonth: v(0),
            year: savedYear,
            entryYear: savedYear,

            province: v(28),              // AC
            configuredProvince: configuredProvince,
            appType: sheetName,           // NEW / RENEW

            workProgram: v(1),            // B
            curCode: v(2),                // C
            lastName: v(3),               // D
            firstName: v(4),              // E
            middleName: v(5),             // F
            suffix: v(6),                 // G
            fullName: v(7),               // H
            address: v(8),                // I
            dob: v(9),                    // J
            age: v(10),                   // K
            gender: v(11),                // L
            education: v(12),             // M
            pwd: v(13),                   // N
            aralProgram: v(14),           // O
            credentialType: v(15),        // P
            trainingTitle: v(16),         // Q
            emailAddress: v(17),          // R
            contactNumber: v(18),         // S
            docsSubmitted: v(19),         // T
            govtAgency: v(20),            // U
            placeOfAssignment: v(21),     // V
            natureOfWork: v(22),          // W
            startDate: startDate,          // X
            endDate: endDate,              // Y
            workMonths: v(25),            // Z
            monthsCount: v(25),           // Z
            contractStatus: v(26),        // AA = Remarks
            remarks: v(26),               // AA
            replacedIntern: v(27),        // AB
            sheetProvince: v(28),         // AC
            idType: v(32),                // AG
            idNumber: v(33),              // AH
            datePlaceOfIssue: v(34)       // AI
          });
        });
      }

      // Read ALL configured province/RO spreadsheets and BOTH tabs.
      const jobs = [];

      for (const [province, spreadsheetId] of Object.entries(GIP_ENTRY_SPREADSHEET_IDS)) {
        for (const sheetName of ['NEW', 'RENEW']) {
          jobs.push(
            readOneTab(province, spreadsheetId, sheetName)
              .catch(err => {
                errors.push(err.message);
                console.error(
                  'GIP Summary read failed:',
                  province,
                  sheetName,
                  err
                );
              })
          );
        }
      }

      await Promise.all(jobs);

      // Stable order: configured province, NEW before RENEW, then source row.
      const provinceOrder = Object.keys(GIP_ENTRY_SPREADSHEET_IDS);
      allRecords.sort((a, b) => {
        const pa = provinceOrder.indexOf(a.configuredProvince);
        const pb = provinceOrder.indexOf(b.configuredProvince);
        if (pa !== pb) return pa - pb;

        const ta = a.appType === 'NEW' ? 0 : 1;
        const tb = b.appType === 'NEW' ? 0 : 1;
        if (ta !== tb) return ta - tb;

        return Number(a.sourceRow || 0) - Number(b.sourceRow || 0);
      });

      console.log(
        'GIP SUMMARY: loaded',
        allRecords.length,
        'data row(s) from A15:AF across all NEW/RENEW sheets.'
      );

      if (errors.length) {
        console.warn(
          'GIP SUMMARY: some sheets could not be read:',
          errors
        );
      }

      return allRecords;
    }

    // ============================================================================
    // GOOGLE SCRIPT RUN POLYFILL BRIDGE
    // ============================================================================
    async function callGipAppsScriptAction(action, payload) {
      const response = await fetch(GIP_SAVE_APPS_SCRIPT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, ...(payload || {}) }),
        redirect: 'follow'
      });
      const text = await response.text();
      let result;
      try { result = JSON.parse(text); }
      catch (_) { throw new Error('Invalid Apps Script response: ' + text.slice(0, 300)); }
      if (!response.ok) throw new Error(result.message || ('HTTP ' + response.status));
      return result;
    }

    async function updateGipBeneficiaryRecord(updatedData) {
      const province = normalizeGipProvince(updatedData && (updatedData.configuredProvince || updatedData.province));
      const spreadsheetId = GIP_ENTRY_SPREADSHEET_IDS[province];
      if (!spreadsheetId) return { success:false, message:'Invalid province/location.' };
      return callGipAppsScriptAction('updateGipBeneficiaryRecord', {
        spreadsheetId,
        sheetName: String(updatedData.appType || '').toUpperCase(),
        updatedData
      });
    }

    async function deleteGipBeneficiaryRecord(deleteData) {
      const province = normalizeGipProvince(deleteData && (deleteData.configuredProvince || deleteData.province));
      const spreadsheetId = GIP_ENTRY_SPREADSHEET_IDS[province];
      if (!spreadsheetId) return { success:false, message:'Invalid province/location.' };
      return callGipAppsScriptAction('deleteGipBeneficiaryRecord', {
        spreadsheetId,
        sheetName: String(deleteData.appType || '').toUpperCase(),
        deleteData
      });
    }


    const apiHandlers = {

      // GIP.html Submit Entry -> province spreadsheet -> NEW/RENEW tab
      processForm:
        async (formData) =>
          processGipForm(formData),

      // GIP.html CUR dropdown:
      // WORK PROGRAM sheet -> Column O
      getCurCodeData:
        async () =>
          getCurCodeData(),

      // GIP.html full-name auto-fill source:
      // FORM RESPONSES 1 -> AA = Full Name, AB = Month
      getMasterListForAutoFill:
        async (selectedMonth) =>
          getMasterListForAutoFill(selectedMonth),

      // Summary tab: consolidate BATANGAS, CAVITE, LAGUNA, QUEZON, RIZAL
      // and REGIONAL OFFICE, including BOTH NEW and RENEW tabs.
      getSummaryData:
        async () =>
          getGipSummaryData(),

      updateBeneficiaryRecord:
        async (updatedData) =>
          updateGipBeneficiaryRecord(updatedData),

      deleteBeneficiaryRecord:
        async (deleteData) =>
          deleteGipBeneficiaryRecord(deleteData),

      getLoggedInUser:
        async () =>
          "jobfairscalabarzon@gmail.com",

      getSheetData:
        async () =>
          getJfRecords(),

      uploadAttachmentToDrive:
        async (
          b64,
          name,
          mime,
          type
        ) =>
          uploadAttachmentToDrive(
            b64,
            name,
            mime,
            type
          ),

      getVenues:
        async () =>
          dbVenues
    };


    // ============================================================================
    // GIP.HTML GOOGLE.SCRIPT.RUN COMPATIBILITY ENDPOINT
    // ============================================================================
    // GIP.html sends:
    // POST /api/gip
    // { "action": "getCurCodeData", "args": [] }
    //
    // This route dispatches the requested action to apiHandlers.
    app.post(
      '/api/gip',
      async (req, res) => {
        try {
          const action =
            String(
              (req.body && req.body.action) || ''
            ).trim();

          const args =
            req.body && Array.isArray(req.body.args)
              ? req.body.args
              : [];

          if (!action) {
            return res.status(400).json({
              __githubApiEnvelope: true,
              success: false,
              message: 'API action is required.'
            });
          }

          const handler = apiHandlers[action];

          if (typeof handler !== 'function') {
            return res.status(404).json({
              __githubApiEnvelope: true,
              success: false,
              message: 'Unknown GIP API action: ' + action
            });
          }

          const data = await handler(...args);

          return res.json({
            __githubApiEnvelope: true,
            success: true,
            data
          });

        } catch (err) {
          console.error(
            'Error executing /api/gip action:',
            err
          );

          return res.status(500).json({
            __githubApiEnvelope: true,
            success: false,
            message:
              err.message ||
              'GIP API request failed.'
          });
        }
      }
    );


    // ============================================================================
    // GENERIC API HANDLER
    // ============================================================================
    app.post(
      '/api/:functionName',
      async (req, res) => {

        const funcName =
          req.params.functionName;


        const args =
          req.body.args ||
          [];


        if (
          apiHandlers[
            funcName
          ]
        ) {

          try {

            const result =
              await apiHandlers[
                funcName
              ](
                ...args
              );


            return res.json({
              result
            });

          } catch (err) {

            console.error(
              `Error executing API function ${funcName}:`,
              err
            );


            return res.status(500).json({

              error:
                err.message

            });
          }
        }


        return res.json({
          result: null
        });
      }
    );


    // ============================================================================
    // ROOT ROUTE
    // ============================================================================
    app.get(
      '/',
      handleDoGet
    );


    // ============================================================================
    // JOB FAIRS ROUTE
    // ============================================================================
    app.get(
      '/jobfairs',
      (req, res) => {

        const output =
          tryRenderHtmlOutput(
            [
              'JFEncoding',
              'jfencoding'
            ],
            'Job Fairs System',
            req
          );


        if (output) {
          return res.send(
            output
          );
        }


        return res.status(404).send(
          'JFEncoding.html not found.'
        );
      }
    );


    // ============================================================================
    // Explicit SPRS filename routes.
    // express.static() normally serves /SPRS.html, but these routes guarantee
    // that the exact file is found on case-sensitive hosts as well.
    app.get(
      '/SPRS.html',
      (req, res) => {
        const filePath = path.join(
          PUBLIC_DIR,
          'SPRS.html'
        );

        if (!fs.existsSync(filePath)) {
          return res.status(404).send(
            'SPRS.html not found in the public directory.'
          );
        }

        return res.sendFile(filePath);
      }
    );

    app.get(
      '/sprs.html',
      (req, res) => {
        const filePath = path.join(
          PUBLIC_DIR,
          'SPRS.html'
        );

        if (!fs.existsSync(filePath)) {
          return res.status(404).send(
            'SPRS.html not found in the public directory.'
          );
        }

        return res.sendFile(filePath);
      }
    );

    // SPRS DIRECT ROUTE
    // ============================================================================
    // Supports:
    //
    // /sprs
    // /SPRS
    // /sprs.html
    // /SPRS.html
    // ============================================================================
    app.get(
      '/sprs',
      (req, res) => {

        req.query.page =
          'sprs';

        return handleDoGet(
          req,
          res
        );
      }
    );


    app.get(
      '/SPRS',
      (req, res) => {

        req.query.page =
          'sprs';

        return handleDoGet(
          req,
          res
        );
      }
    );


    // ============================================================================
    // PAGE ROUTE FALLBACK
    // ============================================================================
    app.get(
      '/:pageName',
      (req, res, next) => {

        let rawPage =
          req.params.pageName ||
          '';


        let cleanPage =
          rawPage.toLowerCase();


        if (
          cleanPage.endsWith(
            '.html'
          )
        ) {

          cleanPage =
            cleanPage.substring(
              0,
              cleanPage.length - 5
            );
        }


        if (
          PAGE_CONFIG[
            cleanPage
          ] ||
          PAGE_CONFIG[
            rawPage.toLowerCase()
          ]
        ) {

          req.query.page =
            cleanPage;


          return handleDoGet(
            req,
            res
          );
        }


        next();
      }
    );


    // ============================================================================
    // SHARED JOB FAIR CALENDAR ADDED STATUS (isolated feature)
    // Requires REDIS_URL for durable status shared across Render instances.
    // ============================================================================
    const JF_CALENDAR_ADMINS = new Set(['FRANCIS','PAUL','KEANNU','JM','CIS','DOLE4A']);
    const JF_CALENDAR_REDIS_KEY = 'epms:jf:calendar:added:v1';
    function jfCalendarUser(req) {
      return verifyAuthCookieValue(readCookie(req, 'dole_auth'));
    }
    function jfCalendarKey(value) {
      const key = String(value == null ? '' : value).trim();
      return key.length > 0 && key.length <= 500 ? key : null;
    }
    app.get('/api/jf/calendar-added', async (req, res) => {
      if (!jfCalendarUser(req)) return res.status(401).json({success:false,message:'Login required.'});
      if (!sharedRedisReady || !sharedRedis) return res.status(503).json({success:false,message:'Shared Redis storage unavailable. Configure REDIS_URL.'});
      try {
        const data = await sharedRedis.hGetAll(JF_CALENDAR_REDIS_KEY);
        const added = Object.fromEntries(Object.entries(data).map(([key, value]) => [key, true]));
        return res.json({success:true,marks:added,added,keys:Object.keys(added)});
      } catch (err) {
        console.error('Calendar status read failed:', err);
        return res.status(503).json({success:false,message:'Unable to read calendar status.'});
      }
    });
    app.post('/api/jf/calendar-added', async (req, res) => {
      const username = jfCalendarUser(req);
      if (!username) return res.status(401).json({success:false,message:'Login required.'});
      if (!JF_CALENDAR_ADMINS.has(String(username).trim().toUpperCase()))
        return res.status(403).json({success:false,message:'Not authorized to mark calendar events.'});
      if (!sharedRedisReady || !sharedRedis) return res.status(503).json({success:false,message:'Shared Redis storage unavailable. Configure REDIS_URL.'});
      const body = req.body || {};
      const keys = Array.isArray(body.keys) ? body.keys : [body.key || body.id || body.eventKey];
      if (!keys.length || keys.length > 1000) return res.status(400).json({success:false,message:'Provide 1 to 1000 event keys.'});
      const normalized = keys.map(jfCalendarKey);
      if (normalized.some(key => !key)) return res.status(400).json({success:false,message:'Invalid event key.'});
      try {
        const entries = Object.fromEntries(normalized.map(key => [key, JSON.stringify({by:username,at:new Date().toISOString()})]));
        await sharedRedis.hSet(JF_CALENDAR_REDIS_KEY, entries);
        return res.json({success:true,keys:normalized,added:true});
      } catch (err) {
        console.error('Calendar status write failed:', err);
        return res.status(503).json({success:false,message:'Unable to save calendar status.'});
      }
    });


    // ============================================================================
    // 404 HANDLER
    // ============================================================================
    app.use(
      (req, res) => {

        if (
          req.path.startsWith(
            '/api/'
          )
        ) {

          return res.status(404).json({

            success: false,

            message:
              'API endpoint not found.',

            path:
              req.path

          });
        }


        return res.status(404).send(
          `<h3>Page Not Found</h3>
           <p>The requested page does not exist.</p>`
        );
      }
    );


    // ============================================================================
    // GLOBAL ERROR HANDLER
    // ============================================================================
    app.use(
      (err, req, res, next) => {

        console.error(
          'Unhandled server error:',
          err
        );


        if (
          res.headersSent
        ) {
          return next(err);
        }


        return res.status(500).json({

          success: false,

          message:
            err.message ||
            'Internal server error.'

        });
      }
    );


    // ============================================================================
    // SERVER LAUNCH
    // ============================================================================
    app.listen(
      PORT,
      () => {

        console.log(
          '===================================================='
        );

        console.log(
          ' DOLE / PESO Job Fairs Express Server Running'
        );

        console.log(
          ` Server URL: http://localhost:${PORT}`
        );

        console.log(
          ' Auth: login | setup-email | forgot-password (OTP)'
        );

        console.log(
          ' SPRS: /sprs'
        );
        console.log(
          ' SPRS: /SPRS.html'
        );

        console.log(
          ' SPRS API: /api/provinces'
        );

        console.log(
          ' SPRS API: /api/job-fair/consolidated'
        );

        console.log(
          ` JF Database: ${JF_DB_PATH}`
        );

        console.log(
          '===================================================='
        );
      }
    );
