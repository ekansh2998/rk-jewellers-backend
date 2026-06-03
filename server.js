require("dotenv").config();

const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");
const axios = require("axios");
const zlib = require("zlib");
const protobuf = require("protobufjs");
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 4000;
const UPSTOX_AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize";
const UPSTOX_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token";
const renderEnvAccessToken = process.env.UPSTOX_ACCESS_TOKEN || process.env.UPSTOX_accessToken || null;
let accessToken = null;
let activeTokenSource = "none";
let memoryBackupAccessToken = renderEnvAccessToken || null;
let refreshToken = null; // Upstox does not provide refresh-token auto renewal in this setup.
let accessTokenExpiresAt = process.env.UPSTOX_TOKEN_EXPIRES_AT || null;
let accessTokenUpdatedAt = process.env.UPSTOX_TOKEN_UPDATED_AT || null;
let accessTokenGeneratedBy = process.env.UPSTOX_TOKEN_GENERATED_BY || process.env.TOKEN_GENERATED_BY || null;
let lastAutoRefreshAt = null;
const CACHE_FILE = path.join(__dirname, "rates-cache.json");
const TOKEN_FILE = path.join(__dirname, "upstox-token.json");

const MONGODB_URI = process.env.MONGODB_URI || "";
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME || "rk_jewellers";
const TOKEN_COLLECTION = process.env.TOKEN_COLLECTION || "upstox_tokens";
const TOKEN_DOC_ID = "main-upstox-token";

let mongoClient = null;
let tokenCollection = null;
let mongoLastError = null;

const UPSTOX_API_KEY = process.env.UPSTOX_API_KEY || process.env.UPSTOX_CLIENT_ID || process.env.API_KEY || "";
const UPSTOX_API_SECRET = process.env.UPSTOX_API_SECRET || process.env.CLIENT_SECRET || process.env.API_SECRET || "";
const UPSTOX_REDIRECT_URI = process.env.UPSTOX_REDIRECT_URI || "";
const ADMIN_ACCESS_PASSWORD = process.env.ADMIN_ACCESS_PASSWORD || "Ekansh2998";
const ADMIN_UPDATE_PASSWORD = process.env.ADMIN_UPDATE_PASSWORD || "Widber";
const JWT_SECRET = process.env.JWT_SECRET || process.env.ADMIN_JWT_SECRET || crypto.createHash("sha256").update(String(UPSTOX_API_SECRET || UPSTOX_API_KEY || "rk-jewellers-local-secret")).digest("hex");
const JWT_EXPIRY_SECONDS = Number(process.env.JWT_EXPIRY_SECONDS || 60 * 60);

// Real admin JWT session state for status visibility.
// Protected admin routes still require a valid Bearer JWT, but these values
// let /api/rates and /api/upstox/status show whether an admin session is active.
let lastAdminSessionToken = null;
let lastAdminSessionPayload = null;
let lastAdminSessionExpiresAt = null;

let tokenNeedsReconnect = false;
let tokenLastError = null;
let currentUpstoxWs = null;
let upstoxWsAlive = false;
let upstoxWsConnecting = false;
let lastWebSocketMessageAt = null;
let lastWebSocketPongAt = null;
let liveFeedStatus = "Not connected";
let websocketReconnectCount = 0;
let lastHeartbeatCheckAt = null;
let tokenAutoRefreshEnabled = false; // intentionally disabled: Upstox refresh-token auto renewal is not supported here.

// Metal rate difference from .env. You can keep blank/0 and control from frontend/API.
let goldDifference = Number(process.env.GOLD_RATE_DIFFERENCE || 0);
let silverDifference = Number(process.env.SILVER_RATE_DIFFERENCE || 0);
let goldDifferenceUpdatedAt = process.env.GOLD_RATE_DIFFERENCE_UPDATED_AT || null;
let silverDifferenceUpdatedAt = process.env.SILVER_RATE_DIFFERENCE_UPDATED_AT || null;
let goldDifferenceMcxAtUpdate = Number(process.env.GOLD_RATE_DIFFERENCE_MCX_AT_UPDATE || 0) || null;
let silverDifferenceMcxAtUpdate = Number(process.env.SILVER_RATE_DIFFERENCE_MCX_AT_UPDATE || 0) || null;

let GOLD_KEY = null;
let SILVER_KEY = null;

let latestRates = {
  goldMcx: null,
  silverMcx: null,
  goldOpen: null,
  silverOpen: null,
  goldPrevClose: null,
  silverPrevClose: null,
  goldThirdLastClose: Number(process.env.GOLD_THIRD_LAST_CLOSE || 0) || null,
  silverThirdLastClose: Number(process.env.SILVER_THIRD_LAST_CLOSE || 0) || null,
  marketClosed: false,
  marketClosedMessage: null,
  marketClosedReferenceMode: "previous-close",
  goldHigh: null,
  silverHigh: null,
  goldLow: null,
  silverLow: null,
  lastUpdated: null,
  source: "waiting",
  status: "Server started. Waiting for Upstox feed.",
};

function parseJsonEnv(key, fallback = null) {
  try {
    const raw = process.env[key];
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

let lastRecordedRates = parseJsonEnv("LAST_RECORDED_RATES_JSON", null);
let lastRecordedRatesUpdatedAt = process.env.LAST_RECORDED_RATES_UPDATED_AT || lastRecordedRates?.lastUpdated || null;
let lastRecordedRenderSyncAt = 0;

function tokenExpiryState() {
  if (!accessTokenExpiresAt) return { expired: false, valid: Boolean(accessToken), label: "Not available" };
  const t = new Date(accessTokenExpiresAt).getTime();
  if (!Number.isFinite(t)) return { expired: false, valid: Boolean(accessToken), label: "Not available" };
  const expired = Date.now() >= t;
  return { expired, valid: Boolean(accessToken) && !expired && !tokenNeedsReconnect, label: expired ? "TOKEN EXPIRED OR NOT WORKING" : accessTokenExpiresAt };
}

function tokensWorking() {
  const state = tokenExpiryState();
  return Boolean(accessToken) && !tokenNeedsReconnect && !state.expired;
}

function normalizeRecordedRates(data) {
  if (!data) return null;
  return {
    goldMcx: Number(data.goldMcx),
    silverMcx: Number(data.silverMcx),
    goldOpen: Number(data.goldOpen),
    silverOpen: Number(data.silverOpen),
    goldPrevClose: Number(data.goldPrevClose),
    silverPrevClose: Number(data.silverPrevClose),
    goldThirdLastClose: Number(data.goldThirdLastClose),
    silverThirdLastClose: Number(data.silverThirdLastClose),
    marketClosed: Boolean(data.marketClosed),
    marketClosedMessage: data.marketClosedMessage || null,
    marketClosedReferenceMode: data.marketClosedReferenceMode || null,
    goldHigh: Number(data.goldHigh),
    silverHigh: Number(data.silverHigh),
    goldLow: Number(data.goldLow),
    silverLow: Number(data.silverLow),
    lastUpdated: data.lastUpdated || data.recordedAt || new Date().toISOString(),
    recordedAt: data.recordedAt || data.lastUpdated || new Date().toISOString(),
  };
}

async function recordLastGoodRatesToRender(reason = "live-rate") {
  if (!tokensWorking()) return;
  if (latestRates.goldMcx == null && latestRates.silverMcx == null) return;
  const now = Date.now();
  // Render Environment API is not designed for every-second writes. 60 sec throttling keeps it safe and reliable.
  if (now - lastRecordedRenderSyncAt < 60000) return;
  lastRecordedRenderSyncAt = now;
  lastRecordedRates = normalizeRecordedRates({ ...latestRates, recordedAt: new Date().toISOString() });
  lastRecordedRatesUpdatedAt = lastRecordedRates.recordedAt;
  process.env.LAST_RECORDED_RATES_JSON = JSON.stringify(lastRecordedRates);
  process.env.LAST_RECORDED_RATES_UPDATED_AT = lastRecordedRatesUpdatedAt;
  try { await updateRenderEnvironmentVariable("LAST_RECORDED_RATES_JSON", JSON.stringify(lastRecordedRates)); } catch (e) { console.log("Could not sync last recorded rates JSON to Render:", e.message); }
  try { await updateRenderEnvironmentVariable("LAST_RECORDED_RATES_UPDATED_AT", lastRecordedRatesUpdatedAt); } catch (e) { console.log("Could not sync last recorded rates time to Render:", e.message); }
}


function getIstClockParts() {
  const parts = new Intl.DateTimeFormat("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  }).formatToParts(new Date());
  const hour = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const minute = Number(parts.find((p) => p.type === "minute")?.value || 0);
  return { hour, minute };
}

let marketClosedAfter1159 = false;
let marketClosedBaselineGold = null;
let marketClosedBaselineSilver = null;

function isAfter1159PmOrOvernightIst() {
  const { hour, minute } = getIstClockParts();
  // Start showing the closed state after 11:59 PM and keep it during the overnight period
  // until a fresh gold/silver tick changes the MCX rate.
  return (hour === 23 && minute >= 59) || hour < 9;
}

function refreshMarketClosedState() {
  if (isAfter1159PmOrOvernightIst() && !marketClosedAfter1159) {
    marketClosedAfter1159 = true;
    marketClosedBaselineGold = Number.isFinite(Number(latestRates.goldMcx)) ? Number(latestRates.goldMcx) : null;
    marketClosedBaselineSilver = Number.isFinite(Number(latestRates.silverMcx)) ? Number(latestRates.silverMcx) : null;
  }
  latestRates.marketClosed = Boolean(marketClosedAfter1159);
  latestRates.marketClosedMessage = marketClosedAfter1159 ? "MARKET CLOSED" : null;
  latestRates.marketClosedReferenceMode = marketClosedAfter1159 ? "third-last-trading-close" : "previous-trading-close";
  return latestRates.marketClosed;
}

function clearMarketClosedIfRateChanged(nextGold, nextSilver) {
  if (!marketClosedAfter1159) return;
  const g = Number(nextGold);
  const s = Number(nextSilver);
  const goldChanged = Number.isFinite(g) && marketClosedBaselineGold != null && g !== marketClosedBaselineGold;
  const silverChanged = Number.isFinite(s) && marketClosedBaselineSilver != null && s !== marketClosedBaselineSilver;
  if (goldChanged || silverChanged) {
    marketClosedAfter1159 = false;
    marketClosedBaselineGold = null;
    marketClosedBaselineSilver = null;
    latestRates.marketClosed = false;
    latestRates.marketClosedMessage = null;
    latestRates.marketClosedReferenceMode = "previous-trading-close";
  }
}

function setPreviousCloseWithHistory(metal, close) {
  const c = Number(close);
  if (!Number.isFinite(c) || c <= 0) return false;
  const prevKey = `${metal}PrevClose`;
  const thirdKey = `${metal}ThirdLastClose`;
  const oldPrev = Number(latestRates[prevKey]);
  if (Number.isFinite(oldPrev) && oldPrev > 0 && oldPrev !== c) {
    latestRates[thirdKey] = oldPrev;
  }
  latestRates[prevKey] = c;
  return true;
}

function shouldShowLastRecordedRates() {
  return !tokensWorking() && lastRecordedRates && (lastRecordedRates.goldMcx || lastRecordedRates.silverMcx);
}

function loadCachedRates() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return;
    const cached = JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"));
    latestRates = { ...latestRates, ...cached, source: "cache", status: "Showing saved last available rates until live MCX starts." };
    console.log("Loaded saved last available rates from cache.");
  } catch (error) {
    console.log("Could not load saved rates cache:", error.message);
  }
}

function saveCachedRates() {
  try {
    if (latestRates.goldMcx == null && latestRates.silverMcx == null) return;
    fs.writeFileSync(CACHE_FILE, JSON.stringify(latestRates, null, 2));
  } catch (error) {
    console.log("Could not save rates cache:", error.message);
  }
}

loadCachedRates();

async function initMongoTokenStore() {
  if (!MONGODB_URI) {
    console.log("MongoDB token storage not configured. Add MONGODB_URI for permanent token storage.");
    return;
  }

  try {
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 4000, connectTimeoutMS: 4000 });
    await mongoClient.connect();
    tokenCollection = mongoClient.db(MONGODB_DB_NAME).collection(TOKEN_COLLECTION);
    mongoLastError = null;
    console.log("MongoDB token storage connected.");
  } catch (error) {
    tokenCollection = null;
    mongoLastError = error.message;
    console.log("MongoDB token storage connection failed:", error.message);
  }
}

function applySavedTokenData(saved, sourceLabel) {
  if (!saved) return false;
  const savedAccess = saved.access_token || saved.accessToken;
  const savedRefresh = saved.refresh_token || saved.refreshToken;
  const savedExpiresAt = saved.expires_at || saved.expiresAt || saved.accessTokenExpiresAt;
  const savedUpdatedAt = saved.updatedAt || saved.savedAt || saved.accessTokenUpdatedAt;
  const savedGeneratedBy = saved.generatedBy || saved.tokenGeneratedBy || saved.source || saved.method || null;

  if (savedAccess) {
    accessToken = savedAccess;
    memoryBackupAccessToken = savedAccess;
    activeTokenSource = sourceLabel === "MongoDB" ? "mongodb" : (sourceLabel === "Render environment" ? "render-env-backup" : "server-memory-backup");
  }
  if (savedRefresh) refreshToken = savedRefresh;
  const decodedSavedExpiry = decodeJwtExpiry(savedAccess);
  if (decodedSavedExpiry) accessTokenExpiresAt = decodedSavedExpiry;
  else if (savedExpiresAt) accessTokenExpiresAt = savedExpiresAt;
  if (savedUpdatedAt) accessTokenUpdatedAt = savedUpdatedAt;
  if (savedGeneratedBy) accessTokenGeneratedBy = savedGeneratedBy;
  if (Number.isFinite(Number(saved.goldPrevClose))) latestRates.goldPrevClose = Number(saved.goldPrevClose);
  if (Number.isFinite(Number(saved.silverPrevClose))) latestRates.silverPrevClose = Number(saved.silverPrevClose);
  if (saved.goldPrevCloseDate) latestRates.goldPrevCloseDate = saved.goldPrevCloseDate;
  if (saved.silverPrevCloseDate) latestRates.silverPrevCloseDate = saved.silverPrevCloseDate;
  if (Number.isFinite(Number(saved.goldThirdLastClose))) latestRates.goldThirdLastClose = Number(saved.goldThirdLastClose);
  if (Number.isFinite(Number(saved.silverThirdLastClose))) latestRates.silverThirdLastClose = Number(saved.silverThirdLastClose);
  if (saved.goldThirdLastCloseDate) latestRates.goldThirdLastCloseDate = saved.goldThirdLastCloseDate;
  if (saved.silverThirdLastCloseDate) latestRates.silverThirdLastCloseDate = saved.silverThirdLastCloseDate;

  if (accessToken) {
    tokenNeedsReconnect = false;
    tokenLastError = null;
    tokenAutoRefreshEnabled = Boolean(refreshToken);
    console.log(`Loaded saved Upstox token data from ${sourceLabel}. Auto refresh: ${tokenAutoRefreshEnabled ? "enabled" : "not available"}.`);
    return true;
  }
  return false;
}

function readTokenFileData() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) return null;
    return JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8"));
  } catch (error) {
    console.log("Could not read saved Upstox token file:", error.message);
    return null;
  }
}

async function loadSavedAccessToken() {
  const tokenFileData = readTokenFileData();
  const fileAccess = tokenFileData?.access_token || tokenFileData?.accessToken;
  if (fileAccess) memoryBackupAccessToken = fileAccess;

  try {
    if (tokenCollection) {
      const saved = await tokenCollection.findOne({ _id: TOKEN_DOC_ID });
      if (applySavedTokenData(saved, "MongoDB")) return;
    }
  } catch (error) {
    mongoLastError = error.message;
    console.log("Could not load Upstox token from MongoDB:", error.message);
  }

  if (renderEnvAccessToken && renderEnvAccessToken !== "0") {
    accessToken = renderEnvAccessToken;
    activeTokenSource = "render-env-backup";
    tokenNeedsReconnect = false;
    tokenLastError = null;
    console.log("MongoDB unavailable. Using Upstox token from Render environment backup.");
    return;
  }

  if (tokenFileData && applySavedTokenData(tokenFileData, "token file")) {
    activeTokenSource = "server-memory-backup";
    return;
  }

  if (accessToken) {
    memoryBackupAccessToken = accessToken;
    tokenNeedsReconnect = false;
    tokenLastError = null;
    tokenAutoRefreshEnabled = Boolean(refreshToken);
    console.log(`Using Upstox token from ${activeTokenSource}. Auto refresh: ${tokenAutoRefreshEnabled ? "enabled" : "not available"}.`);
  }
}

function getMongoFallbackMessage() {
  if (tokenCollection) return null;
  if (activeTokenSource === "render-env-backup") return "mangodb gets disconnected and using token value by render";
  return "mangodb gets disconnected and token is not updated on render";
}

function tryMemoryTokenFallback() {
  if (tokenCollection) return false;
  if (activeTokenSource !== "render-env-backup") return false;
  if (!memoryBackupAccessToken || memoryBackupAccessToken === accessToken) return false;
  accessToken = memoryBackupAccessToken;
  activeTokenSource = "server-memory-backup";
  tokenNeedsReconnect = false;
  tokenLastError = null;
  latestRates.status = "Render backup token failed. Trying latest token from server memory.";
  console.log(latestRates.status);
  return true;
}


function base64Url(input) {
  return Buffer.from(JSON.stringify(input)).toString("base64url");
}

function signAdminJwt(payload) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "HS256", typ: "JWT" };
  const body = { ...payload, iat: now, exp: now + JWT_EXPIRY_SECONDS };
  const data = `${base64Url(header)}.${base64Url(body)}`;
  const signature = crypto.createHmac("sha256", JWT_SECRET).update(data).digest("base64url");
  return `${data}.${signature}`;
}

function verifyAdminJwt(token) {
  try {
    const parts = String(token || "").split(".");
    if (parts.length !== 3) return null;
    const data = `${parts[0]}.${parts[1]}`;
    const expected = crypto.createHmac("sha256", JWT_SECRET).update(data).digest("base64url");
    const actual = parts[2];
    if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(actual))) return null;
    const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
    if (!payload.exp || Date.now() >= Number(payload.exp) * 1000) return null;
    return payload;
  } catch (error) {
    return null;
  }
}

function requireAdminJwt(req, res, next) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  const payload = verifyAdminJwt(token);
  if (!payload) return res.status(401).json({ ok: false, message: "Admin session expired or invalid. Enter password again." });
  req.admin = payload;
  next();
}

function getAdminSessionState(req = null) {
  let payload = null;
  if (req) {
    const auth = req.headers.authorization || "";
    const headerToken = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    if (headerToken) payload = verifyAdminJwt(headerToken);
  }

  const memorySessionValid = Boolean(
    lastAdminSessionToken &&
    lastAdminSessionExpiresAt &&
    Date.now() < Number(lastAdminSessionExpiresAt)
  );

  if (!payload && memorySessionValid) payload = lastAdminSessionPayload;

  return {
    jwtEnabled: true,
    adminSession: Boolean(payload || memorySessionValid),
    authVerified: Boolean(payload || memorySessionValid),
    adminSessionExpiresAt: payload?.exp ? new Date(Number(payload.exp) * 1000).toISOString() : (memorySessionValid ? new Date(Number(lastAdminSessionExpiresAt)).toISOString() : null),
  };
}

function rememberAdminSession(token, payload) {
  lastAdminSessionToken = token;
  lastAdminSessionPayload = payload;
  lastAdminSessionExpiresAt = payload?.exp ? Number(payload.exp) * 1000 : Date.now() + JWT_EXPIRY_SECONDS * 1000;
}

function markRenderTokenActiveIfPossible(renderUpdate) {
  // MongoDB is the primary token source. Render ENV is only a backup copy.
  // Earlier this function changed tokenStorage to render-env-backup after every
  // successful Render sync, which made the API look like it was using Render
  // even when MongoDB had saved the token. Keep MongoDB as active source when
  // MongoDB is connected and token is saved there.
  if (renderUpdate?.updated && accessToken) {
    process.env.UPSTOX_ACCESS_TOKEN = accessToken;
  }
  if (tokenCollection && accessToken) {
    activeTokenSource = "mongodb";
    return;
  }
  if (renderUpdate?.updated) {
    activeTokenSource = "render-env-backup";
  } else if (activeTokenSource !== "render-env-backup") {
    activeTokenSource = "server-memory-backup";
  }
}

function decodeJwtExpiry(accessTokenValue) {
  try {
    const parts = String(accessTokenValue || "").split(".");
    if (parts.length < 2) return null;
    const payload = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = payload + "=".repeat((4 - (payload.length % 4)) % 4);
    const decoded = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    if (decoded && Number.isFinite(Number(decoded.exp))) {
      return new Date(Number(decoded.exp) * 1000).toISOString();
    }
    if (decoded && Number.isFinite(Number(decoded.expires_at))) {
      const raw = Number(decoded.expires_at);
      return new Date(raw > 9999999999 ? raw : raw * 1000).toISOString();
    }
  } catch (error) {
    console.log("Could not decode access token expiry:", error.message);
  }
  return null;
}

function buildTokenExpiry(tokenData = {}) {
  const tokenValue = tokenData.access_token || tokenData.accessToken || accessToken;
  const decodedExpiry = decodeJwtExpiry(tokenValue);
  if (decodedExpiry) return decodedExpiry;

  const rawExpiresAt = tokenData.expires_at || tokenData.expiresAt || tokenData.accessTokenExpiresAt;
  if (rawExpiresAt) {
    const parsed = new Date(rawExpiresAt);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }

  const expiresInSeconds = Number(tokenData.expires_in || tokenData.expiresIn);
  if (Number.isFinite(expiresInSeconds) && expiresInSeconds > 0) {
    return new Date(Date.now() + expiresInSeconds * 1000).toISOString();
  }

  // Fallback only when Upstox does not return an expiry and token is not JWT-decodable.
  return new Date(Date.now() + 23 * 60 * 60 * 1000).toISOString();
}

async function saveAccessToken(tokenData) {
  accessToken = tokenData.access_token || tokenData.accessToken || accessToken;
  if (accessToken) {
    memoryBackupAccessToken = accessToken;
    activeTokenSource = tokenCollection ? "mongodb" : "server-memory-backup";
  }
  refreshToken = tokenData.refresh_token || tokenData.refreshToken || refreshToken;
  accessTokenExpiresAt = buildTokenExpiry(tokenData);
  tokenNeedsReconnect = false;
  tokenLastError = null;
  tokenAutoRefreshEnabled = Boolean(refreshToken);
  accessTokenUpdatedAt = new Date().toISOString();
  accessTokenGeneratedBy = tokenData.generatedBy || tokenData.tokenGeneratedBy || tokenData.source || accessTokenGeneratedBy || "unknown";

  const dataToSave = {
    ...tokenData,
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_at: accessTokenExpiresAt,
    autoRefreshEnabled: tokenAutoRefreshEnabled,
    savedAt: accessTokenUpdatedAt,
    updatedAt: accessTokenUpdatedAt,
    generatedBy: accessTokenGeneratedBy,
    tokenGeneratedBy: accessTokenGeneratedBy,
  };

  try {
    if (tokenCollection) {
      await tokenCollection.updateOne(
        { _id: TOKEN_DOC_ID },
        { $set: dataToSave },
        { upsert: true }
      );
      console.log(`Saved Upstox token data to MongoDB. Auto refresh: ${tokenAutoRefreshEnabled ? "enabled" : "not available"}.`);
    }
  } catch (error) {
    console.log("Could not save Upstox token to MongoDB:", error.message);
  }

  try {
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(dataToSave, null, 2));
  } catch (error) {
    console.log("Could not save Upstox token file:", error.message);
  }
}

function isTokenExpiringSoon(bufferMs = 10 * 60 * 1000) {
  if (!accessTokenExpiresAt) return false;
  const expiryTime = new Date(accessTokenExpiresAt).getTime();
  if (!Number.isFinite(expiryTime)) return false;
  return Date.now() + bufferMs >= expiryTime;
}

async function refreshAccessTokenIfPossible(force = false) {
  // Upstox does not currently support a public refresh-token renewal flow for this setup.
  // Keep this function disabled so the backend never tries an impossible auto-refresh.
  tokenAutoRefreshEnabled = false;
  return false;
}


async function ensureValidAccessToken() {
  if (!accessToken) {
    await loadSavedAccessToken();
  }
  if (isTokenExpiringSoon()) {
    await refreshAccessTokenIfPossible(false);
  }
  return Boolean(accessToken);
}

function buildPublicBaseUrl(req) {
  if (UPSTOX_REDIRECT_URI) return UPSTOX_REDIRECT_URI.replace(/\/api\/upstox\/callback\/?$/, "").replace(/\/$/, "");
  const proto = req.headers["x-forwarded-proto"] || req.protocol || "https";
  return `${proto}://${req.get("host")}`;
}

function getRedirectUri(req) {
  return UPSTOX_REDIRECT_URI || `${buildPublicBaseUrl(req)}/api/upstox/callback`;
}

function getUpstoxLoginUrl(req) {
  const redirectUri = getRedirectUri(req);
  const params = new URLSearchParams({
    response_type: "code",
    client_id: UPSTOX_API_KEY,
    redirect_uri: redirectUri,
  });
  return `https://api.upstox.com/v2/login/authorization/dialog?${params.toString()}`;
}

function markTokenReconnectNeeded(message) {
  tokenNeedsReconnect = true;
  tokenLastError = message || "TOKEN EXPIRED OR NOT WORKING";
  latestRates.source = latestRates.goldMcx || latestRates.silverMcx ? "upstox-last-quote" : "token-expired";
  latestRates.status = "TOKEN EXPIRED OR NOT WORKING";
}

const clients = new Set();

const MARKET_DATA_FEED_V3_PROTO = `
syntax = "proto3";
package com.upstox.marketdatafeederv3udapi.rpc.proto;

message LTPC {
  double ltp = 1;
  int64 ltt = 2;
  int64 ltq = 3;
  double cp = 4;
}

message MarketLevel {
  repeated Quote bidAskQuote = 1;
}

message MarketOHLC {
  repeated OHLC ohlc = 1;
}

message Quote {
  int64 bidQ = 1;
  double bidP = 2;
  int64 askQ = 3;
  double askP = 4;
}

message OptionGreeks {
  double delta = 1;
  double theta = 2;
  double gamma = 3;
  double vega = 4;
  double rho = 5;
}

message OHLC {
  string interval = 1;
  double open = 2;
  double high = 3;
  double low = 4;
  double close = 5;
  int64 vol = 6;
  int64 ts = 7;
}

enum Type {
  initial_feed = 0;
  live_feed = 1;
  market_info = 2;
}

message MarketFullFeed {
  LTPC ltpc = 1;
  MarketLevel marketLevel = 2;
  OptionGreeks optionGreeks = 3;
  MarketOHLC marketOHLC = 4;
  double atp = 5;
  int64 vtt = 6;
  double oi = 7;
  double iv = 8;
  double tbq = 9;
  double tsq = 10;
}

message IndexFullFeed {
  LTPC ltpc = 1;
  MarketOHLC marketOHLC = 2;
}

message FullFeed {
  oneof FullFeedUnion {
    MarketFullFeed marketFF = 1;
    IndexFullFeed indexFF = 2;
  }
}

message FirstLevelWithGreeks {
  LTPC ltpc = 1;
  Quote firstDepth = 2;
  OptionGreeks optionGreeks = 3;
  int64 vtt = 4;
  double oi = 5;
  double iv = 6;
}

message Feed {
  oneof FeedUnion {
    LTPC ltpc = 1;
    FullFeed fullFeed = 2;
    FirstLevelWithGreeks firstLevelWithGreeks = 3;
  }
  RequestMode requestMode = 4;
}

enum RequestMode {
  ltpc = 0;
  full_d5 = 1;
  option_greeks = 2;
  full_d30 = 3;
}

enum MarketStatus {
  PRE_OPEN_START = 0;
  PRE_OPEN_END = 1;
  NORMAL_OPEN = 2;
  NORMAL_CLOSE = 3;
  CLOSING_START = 4;
  CLOSING_END = 5;
}

message MarketInfo {
  map<string, MarketStatus> segmentStatus = 1;
}

message FeedResponse {
  Type type = 1;
  map<string, Feed> feeds = 2;
  int64 currentTs = 3;
  MarketInfo marketInfo = 4;
}
`;

const protoRoot = protobuf.parse(MARKET_DATA_FEED_V3_PROTO).root;
const FeedResponse = protoRoot.lookupType(
  "com.upstox.marketdatafeederv3udapi.rpc.proto.FeedResponse"
);

function decodeUpstoxBinaryFeed(buffer) {
  const decoded = FeedResponse.decode(buffer);
  return FeedResponse.toObject(decoded, {
    longs: String,
    enums: String,
    defaults: false,
  });
}



async function getAutoKeys() {
  const url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz";

  const res = await axios.get(url, { responseType: "arraybuffer", timeout: 30000 });
  const json = zlib.gunzipSync(res.data).toString("utf8");
  const instruments = JSON.parse(json);

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const fiveDaysMs = 5 * 24 * 60 * 60 * 1000;
  // Any contract expiring in the next 5 calendar days must be skipped.
  // Example: 1 June 2026 + 5-day buffer means 5 June 2026 expiry is skipped.
  const minimumAllowedExpiry = today.getTime() + fiveDaysMs;

  function parseYYYYMMDD(value) {
    const text = String(value || "").trim();
    if (!/^\d{8}$/.test(text)) return 0;
    const y = Number(text.slice(0, 4));
    const m = Number(text.slice(4, 6)) - 1;
    const d = Number(text.slice(6, 8));
    const t = new Date(y, m, d).getTime();
    return Number.isFinite(t) ? t : 0;
  }

  function expiryTime(x) {
    const raw = x?.expiry || x?.expiry_date || x?.expiryDate || x?.contract_expiry;
    const ymd = parseYYYYMMDD(raw);
    if (ymd) return ymd;

    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) {
      const digits = String(Math.trunc(n)).length;
      if (digits === 8) return parseYYYYMMDD(String(Math.trunc(n)));
      if (digits <= 10) return n * 1000;       // seconds
      if (digits <= 13) return n;              // milliseconds
      if (digits <= 16) return Math.floor(n / 1000); // microseconds
      return Math.floor(n / 1000000);          // nanoseconds
    }

    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeText(value) {
    return String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  }

  function textPack(x) {
    return [
      x.trading_symbol, x.tradingsymbol, x.symbol, x.name,
      x.asset_symbol, x.underlying_symbol, x.underlying, x.instrument_type,
      x.instrumentType, x.segment, x.exchange_segment, x.exchange
    ].map(normalizeText).join("|");
  }

  function isMainCommodityFuture(x, symbol) {
    const segment = String(x.segment || x.exchange_segment || x.exchange || "").toUpperCase();
    const type = String(x.instrument_type || x.instrumentType || x.instrument_type_name || "").toUpperCase();
    const assetSymbol = normalizeText(x.asset_symbol || x.underlying_symbol || x.underlying || x.assetSymbol);
    const name = normalizeText(x.name || x.instrument_name);
    const tradingSymbol = normalizeText(x.trading_symbol || x.tradingsymbol || x.symbol || x.tradingSymbol);
    const pack = textPack(x);

    // Only MCX futures. Reject NSE/BSE, options, call-put and non-futures.
    if (!segment.includes("MCX")) return false;
    if (!(type.includes("FUT") || pack.includes("FUT"))) return false;
    if (type.includes("OPT") || pack.includes("OPTION") || /(^|\|)[A-Z0-9]*(CE|PE)(\||$)/.test(pack)) return false;

    // Reject all non-main bullion contracts.
    const rejectedWords = [
      "GOLDM", "GOLDMINI", "GOLDGUINEA", "GOLDPETAL", "GOLDTEN",
      "SILVERM", "SILVERMINI", "SILVERMIC", "SILVERMICRO", "SILVER100", "SILVER1000",
      "MINI", "MICRO", "PETAL", "GUINEA", "TEN", "1000"
    ];
    if (rejectedWords.some((word) => pack.includes(word))) return false;

    if (symbol === "GOLD") {
      if (assetSymbol && assetSymbol !== "GOLD") return false;
      if (!assetSymbol && !tradingSymbol.startsWith("GOLD") && !name.startsWith("GOLD")) return false;
      return true;
    }

    if (symbol === "SILVER") {
      if (assetSymbol && assetSymbol !== "SILVER") return false;
      if (!assetSymbol && !tradingSymbol.startsWith("SILVER") && !name.startsWith("SILVER")) return false;
      return true;
    }

    return false;
  }

  function find(symbol) {
    const matches = instruments
      .filter((x) => isMainCommodityFuture(x, symbol))
      .map((x) => ({ ...x, __expiryTime: expiryTime(x) }))
      .filter((x) => x.__expiryTime > minimumAllowedExpiry)
      .sort((a, b) => a.__expiryTime - b.__expiryTime);

    if (!matches.length) {
      const sample = instruments
        .filter((x) => String(x.segment || x.exchange_segment || x.exchange || "").toUpperCase().includes("MCX"))
        .filter((x) => textPack(x).includes(symbol))
        .slice(0, 5)
        .map((x) => ({ trading_symbol: x.trading_symbol || x.tradingsymbol || x.symbol, name: x.name, type: x.instrument_type || x.instrumentType, expiry: x.expiry, key: x.instrument_key }))
        .filter(Boolean);
      console.log(`No valid main ${symbol} future found after 5-day rollover filter. Sample:`, sample);
      return null;
    }

    return matches[0];
  }

  const gold = find("GOLD");
  const silver = find("SILVER");

  if (!gold || !silver) {
    throw new Error("Could not find active main MCX GOLD/SILVER futures after applying 5-day expiry skip rule.");
  }

  return { gold, silver };
}

async function prepareInstrumentKeys() {
  const autoDetectEnabled = String(process.env.AUTO_DETECT_KEYS || "true").toLowerCase() !== "false";
  try {
    const autoKeys = await getAutoKeys();

    GOLD_KEY = autoDetectEnabled ? autoKeys.gold.instrument_key : (process.env.MANUAL_GOLD_KEY || autoKeys.gold.instrument_key);
    SILVER_KEY = autoDetectEnabled ? autoKeys.silver.instrument_key : (process.env.MANUAL_SILVER_KEY || autoKeys.silver.instrument_key);

    latestRates.goldContract = autoKeys.gold.trading_symbol || autoKeys.gold.tradingsymbol || autoKeys.gold.symbol || autoKeys.gold.name || "GOLD FUT";
    latestRates.silverContract = autoKeys.silver.trading_symbol || autoKeys.silver.tradingsymbol || autoKeys.silver.symbol || autoKeys.silver.name || "SILVER FUT";
    latestRates.goldContractExpiry = autoKeys.gold.expiry || autoKeys.gold.expiry_date || autoKeys.gold.expiryDate || null;
    latestRates.silverContractExpiry = autoKeys.silver.expiry || autoKeys.silver.expiry_date || autoKeys.silver.expiryDate || null;

    console.log("Using main MCX GOLD contract:", latestRates.goldContract, GOLD_KEY, "expiry:", latestRates.goldContractExpiry);
    console.log("Using main MCX SILVER contract:", latestRates.silverContract, SILVER_KEY, "expiry:", latestRates.silverContractExpiry);

    latestRates.status = "Main MCX GOLD/SILVER contracts loaded. Contracts expiring within next 5 days are skipped.";
  } catch (error) {
    console.log("Auto key download/selection failed:", error.message);

    if (!autoDetectEnabled) {
      GOLD_KEY = process.env.MANUAL_GOLD_KEY || process.env.GOLD_INSTRUMENT_KEY || null;
      SILVER_KEY = process.env.MANUAL_SILVER_KEY || process.env.SILVER_INSTRUMENT_KEY || null;
      latestRates.goldContract = null;
      latestRates.silverContract = null;
      latestRates.goldContractExpiry = null;
      latestRates.silverContractExpiry = null;
      latestRates.status = GOLD_KEY && SILVER_KEY
        ? "Auto-detect disabled. Using manual instrument keys from Render."
        : "Auto-detect disabled and manual keys are missing.";
      return;
    }

    // When auto-detect is enabled, do not silently use old manual keys, because that can keep an expiring/wrong contract active.
    GOLD_KEY = null;
    SILVER_KEY = null;
    latestRates.goldContract = null;
    latestRates.silverContract = null;
    latestRates.goldContractExpiry = null;
    latestRates.silverContractExpiry = null;
    latestRates.status = "Could not auto-select main MCX GOLD/SILVER contracts. Check Upstox instrument master/API token.";
  }
}

function calculateRates(goldMcx, silverMcx, req = null) {
  const tokenState = tokenExpiryState();
  const marketClosed = refreshMarketClosedState();
  const useRecorded = shouldShowLastRecordedRates();
  const sourceRates = useRecorded ? lastRecordedRates : latestRates;
  const gold = Number(useRecorded ? sourceRates.goldMcx : goldMcx);
  const silver = Number(useRecorded ? sourceRates.silverMcx : silverMcx);
  const gDiff = Number(goldDifference || 0);
  const sDiff = Number(silverDifference || 0);

  const gold24k = Number.isFinite(gold) ? (gold + gDiff) / 0.995 : null;
  const silver1kg = Number.isFinite(silver) ? silver + sDiff : null;
  const adminState = getAdminSessionState(req);
  const adminSession = adminState.adminSession;

  return {
    goldMcx: Number.isFinite(gold) ? gold : null,
    silverMcx: Number.isFinite(silver) ? silver : null,
    goldOpen: sourceRates.goldOpen,
    silverOpen: sourceRates.silverOpen,
    goldPrevClose: sourceRates.goldPrevClose,
    silverPrevClose: sourceRates.silverPrevClose,
    goldThirdLastClose: sourceRates.goldThirdLastClose || latestRates.goldThirdLastClose || null,
    silverThirdLastClose: sourceRates.silverThirdLastClose || latestRates.silverThirdLastClose || null,
    goldComparisonClose: marketClosed ? (sourceRates.goldThirdLastClose || latestRates.goldThirdLastClose || sourceRates.goldPrevClose) : sourceRates.goldPrevClose,
    silverComparisonClose: marketClosed ? (sourceRates.silverThirdLastClose || latestRates.silverThirdLastClose || sourceRates.silverPrevClose) : sourceRates.silverPrevClose,
    marketClosed,
    marketClosedMessage: marketClosed ? "MARKET CLOSED" : null,
    marketClosedReferenceMode: marketClosed ? "third-last-trading-close" : "previous-trading-close",
    goldHigh: sourceRates.goldHigh,
    silverHigh: sourceRates.silverHigh,
    goldLow: sourceRates.goldLow,
    silverLow: sourceRates.silverLow,

    goldDifference: gDiff,
    silverDifference: sDiff,
    goldDifferenceUpdatedAt,
    silverDifferenceUpdatedAt,
    goldDifferenceMcxAtUpdate,
    silverDifferenceMcxAtUpdate,

    gold24k,
    gold22k: gold24k != null ? gold24k * 0.916 : null,
    gold20k: gold24k != null ? gold24k * 0.8334 : null,
    gold18k: gold24k != null ? gold24k * 0.75 : null,
    gold14k: gold24k != null ? gold24k * 0.5834 : null,

    silver1kg,
    silver10gram: silver1kg != null ? silver1kg / 100 : null,

    lastUpdated: useRecorded ? sourceRates.lastUpdated : latestRates.lastUpdated,
    source: useRecorded ? "render-last-recorded" : latestRates.source,
    status: useRecorded ? "LAST RECORDED DATA IS SHOWING BECAUSE TOKEN GOT EXPIRED OR INVALID" : latestRates.status,
    showingLastRecordedData: Boolean(useRecorded),
    lastRecordedWarning: useRecorded ? "LAST RECORDED DATA IS SHOWING BECAUSE TOKEN GOT EXPIRED OR INVALID" : null,
    lastRecordedRatesUpdatedAt,
    liveFeedStatus,
    lastWebSocketMessageAt,
    lastWebSocketPongAt,
    websocketReconnectCount,
    lastHeartbeatCheckAt,
    tokenNeedsReconnect: tokenNeedsReconnect || tokenState.expired,
    tokenLastError: tokenState.expired ? "TOKEN EXPIRED OR NOT WORKING" : tokenLastError,
    tokenAutoRefreshEnabled,
    refreshTokenPresent: Boolean(refreshToken),
    accessTokenExpiresAt,
    tokenGeneratedBy: accessTokenGeneratedBy || null,
    tokenExpiryDisplay: tokenState.label,
    tokenExpired: tokenState.expired,
    tokenWorking: tokensWorking(),
    lastAutoRefreshAt,
    reconnectPath: (tokenNeedsReconnect || tokenState.expired) ? "/upstox" : null,
    mongoConnected: Boolean(tokenCollection),
    tokenStorage: activeTokenSource,
    tokenPriority: "mongodb -> render-env-backup -> server-memory-backup",
    usingRenderBackupToken: activeTokenSource === "render-env-backup" && tokensWorking(),
    usingMemoryBackupToken: activeTokenSource === "server-memory-backup" && tokensWorking(),
    renderTokenAccess: activeTokenSource === "render-env-backup" && tokensWorking(),
    backendMemoryTokenAccess: activeTokenSource === "server-memory-backup" && tokensWorking(),
    mongoWarning: getMongoFallbackMessage(),
    mongoLastError,
    jwtEnabled: adminState.jwtEnabled,
    adminSession,
    authVerified: adminState.authVerified,
    adminSessionExpiresAt: adminState.adminSessionExpiresAt,
    goldInstrumentKey: GOLD_KEY,
    silverInstrumentKey: SILVER_KEY,
    goldContract: latestRates.goldContract || null,
    silverContract: latestRates.silverContract || null,
    goldContractExpiry: latestRates.goldContractExpiry || null,
    silverContractExpiry: latestRates.silverContractExpiry || null,
  };
}

function broadcast() {
  const payload = JSON.stringify({
    type: "rates",
    data: calculateRates(latestRates.goldMcx, latestRates.silverMcx),
  });

  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function getFeed(raw, instrumentKey) {
  return raw?.feeds?.[instrumentKey] || raw?.data?.feeds?.[instrumentKey] || raw?.[instrumentKey] || null;
}

function extractLtpFromFeed(raw, instrumentKey) {
  const feed = getFeed(raw, instrumentKey);
  if (!feed) return null;
  const possibleValues = [
    feed?.ltpc?.ltp,
    feed?.fullFeed?.marketFF?.ltpc?.ltp,
    feed?.fullFeed?.indexFF?.ltpc?.ltp,
    feed?.firstLevelWithGreeks?.ltpc?.ltp,
    feed?.ff?.marketFF?.ltpc?.ltp,
    feed?.ff?.indexFF?.ltpc?.ltp,
    feed?.ltp,
  ];
  for (const value of possibleValues) {
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function extractDayOhlcFromFeed(raw, instrumentKey) {
  const feed = getFeed(raw, instrumentKey);
  const ohlcList = feed?.fullFeed?.marketFF?.marketOHLC?.ohlc || feed?.fullFeed?.indexFF?.marketOHLC?.ohlc || feed?.marketOHLC?.ohlc || [];
  const day = ohlcList.find((x) => String(x.interval || "").toLowerCase() === "1d") || ohlcList[0];
  if (!day) return null;
  const open = Number(day.open), high = Number(day.high), low = Number(day.low);
  const close = Number(day.close ?? day.cp ?? day.previousClose ?? day.prev_close);
  return {
    open: Number.isFinite(open) ? open : null,
    close: Number.isFinite(close) ? close : null,
    high: Number.isFinite(high) ? high : null,
    low: Number.isFinite(low) ? low : null,
  };
}

function pickQuoteObject(data, instrumentKey) {
  const root = data?.data || data;
  if (!root) return null;
  if (root[instrumentKey]) return root[instrumentKey];
  return Object.values(root).find((item) =>
    item?.instrument_key === instrumentKey ||
    item?.instrumentKey === instrumentKey ||
    item?.symbol === instrumentKey ||
    item?.instrument_token === instrumentKey
  ) || null;
}

function applyQuoteFallback(quote, metal) {
  if (!quote) return false;
  const ltp = Number(quote.last_price ?? quote.ltp ?? quote.lastPrice ?? quote.close ?? quote.cp);
  const ohlc = quote.ohlc || quote.OHLC || quote.day_ohlc || {};
  const open = Number(ohlc.open ?? quote.open);
  const close = Number(ohlc.close ?? quote.close ?? quote.cp ?? quote.previousClose ?? quote.prev_close);
  const high = Number(ohlc.high ?? quote.high);
  const low = Number(ohlc.low ?? quote.low);

  let updated = false;
  if (Number.isFinite(ltp)) { latestRates[`${metal}Mcx`] = ltp; updated = true; }
  if (Number.isFinite(open)) { latestRates[`${metal}Open`] = open; updated = true; }
  // Do NOT update PrevClose/ThirdLastClose from live quote close.
  // Upstox quote/OHLC close can be today's partial value.
  // Previous/third-last close must come only from daily historical candles.
  if (Number.isFinite(close) && !latestRates[`${metal}PrevClose`]) {
    latestRates[`${metal}PrevClose`] = close;
    updated = true;
  }
  if (Number.isFinite(high)) { latestRates[`${metal}High`] = high; updated = true; }
  if (Number.isFinite(low)) { latestRates[`${metal}Low`] = low; updated = true; }
  return updated;
}

let historicalCloseLastFetchAt = 0;

function formatDateYYYYMMDD(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function getIstDateOnlyString(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit" }).format(date);
}

function candleDateOnlyIST(ts) {
  if (!ts) return null;
  if (typeof ts === "string" && /^\d{4}-\d{2}-\d{2}$/.test(ts)) return ts;
  const d = new Date(ts);
  if (!Number.isNaN(d.getTime())) return getIstDateOnlyString(d);
  const text = String(ts);
  const m = text.match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function normalizeCandleList(raw) {
  const candles = raw?.data?.candles || raw?.candles || raw?.data || [];
  if (!Array.isArray(candles)) return [];
  return candles.map((c) => {
    if (Array.isArray(c)) {
      return { ts: c[0], open: Number(c[1]), high: Number(c[2]), low: Number(c[3]), close: Number(c[4]) };
    }
    return {
      ts: c.ts || c.timestamp || c.time || c.date,
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close ?? c.cp),
    };
  }).filter((c) => Number.isFinite(c.close) && c.close > 0 && c.ts);
}

async function fetchDailyCandlesForInstrument(instrumentKey) {
  if (!instrumentKey || !accessToken) return [];
  const to = new Date();
  const from = new Date(Date.now() - 18 * 24 * 60 * 60 * 1000);
  const toDate = formatDateYYYYMMDD(to);
  const fromDate = formatDateYYYYMMDD(from);
  const encodedKey = encodeURIComponent(instrumentKey);
  const urls = [
    `https://api.upstox.com/v2/historical-candle/${encodedKey}/day/${toDate}/${fromDate}`,
    `https://api.upstox.com/v2/historical-candle/${encodedKey}/1day/${toDate}/${fromDate}`,
  ];

  for (const url of urls) {
    try {
      const response = await axios.get(url, {
        headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
        timeout: 8000,
      });
      const list = normalizeCandleList(response.data);
      if (list.length) return list;
    } catch (error) {
      const msg = error.response?.data?.errors?.[0]?.message || error.response?.data?.message || error.message;
      console.log("Historical candle fetch failed:", msg);
    }
  }
  return [];
}

function applyHistoricalCloses(metal, candles) {
  const todayIst = getIstDateOnlyString();

  // Use ONLY completed historical daily candles.
  // If today candle exists, remove it. This fixes wrong values during market hours.
  // Upstox can return candles in any order, so sort by date descending.
  const completed = candles
    .map((c) => ({ ...c, dateOnly: candleDateOnlyIST(c.ts) }))
    .filter((c) => c.dateOnly && c.dateOnly < todayIst && Number.isFinite(Number(c.close)) && Number(c.close) > 0)
    .sort((a, b) => String(b.dateOnly).localeCompare(String(a.dateOnly)));

  // Example: today 2026-06-03
  // completed[0] = 2026-06-02 close = previous trading day close
  // completed[1] = 2026-06-01 close = third-last close as requested for market-closed comparison
  const previousClose = Number(completed[0]?.close);
  const thirdLastClose = Number(completed[1]?.close);

  let changed = false;
  if (Number.isFinite(previousClose) && previousClose > 0) {
    latestRates[`${metal}PrevClose`] = previousClose;
    latestRates[`${metal}PrevCloseDate`] = completed[0]?.dateOnly || null;
    changed = true;
  }
  if (Number.isFinite(thirdLastClose) && thirdLastClose > 0) {
    latestRates[`${metal}ThirdLastClose`] = thirdLastClose;
    latestRates[`${metal}ThirdLastCloseDate`] = completed[1]?.dateOnly || null;
    changed = true;
  }
  return changed;
}

async function saveHistoricalClosesToMongo() {
  if (!tokenCollection) return;
  const payload = {
    goldPrevClose: latestRates.goldPrevClose || null,
    silverPrevClose: latestRates.silverPrevClose || null,
    goldThirdLastClose: latestRates.goldThirdLastClose || null,
    silverThirdLastClose: latestRates.silverThirdLastClose || null,
    goldPrevCloseDate: latestRates.goldPrevCloseDate || null,
    silverPrevCloseDate: latestRates.silverPrevCloseDate || null,
    goldThirdLastCloseDate: latestRates.goldThirdLastCloseDate || null,
    silverThirdLastCloseDate: latestRates.silverThirdLastCloseDate || null,
    historicalCloseUpdatedAt: new Date().toISOString(),
  };
  try {
    await tokenCollection.updateOne({ _id: TOKEN_DOC_ID }, { $set: payload }, { upsert: true });
  } catch (error) {
    console.log("Could not save historical closes to MongoDB:", error.message);
  }
}

async function refreshHistoricalTradingCloses(force = false) {
  if (!GOLD_KEY || !SILVER_KEY || !accessToken) return false;
  const now = Date.now();
  if (!force && now - historicalCloseLastFetchAt < 10 * 60 * 1000) return false;
  historicalCloseLastFetchAt = now;

  try {
    const [goldCandles, silverCandles] = await Promise.all([
      fetchDailyCandlesForInstrument(GOLD_KEY),
      fetchDailyCandlesForInstrument(SILVER_KEY),
    ]);
    const goldChanged = applyHistoricalCloses("gold", goldCandles);
    const silverChanged = applyHistoricalCloses("silver", silverCandles);
    if (goldChanged || silverChanged) {
      latestRates.historicalCloseUpdatedAt = new Date().toISOString();
      await saveHistoricalClosesToMongo();
      saveCachedRates();
      broadcast();
      console.log("Historical previous/third-last closes updated.", {
        goldPrevClose: latestRates.goldPrevClose,
        goldThirdLastClose: latestRates.goldThirdLastClose,
        silverPrevClose: latestRates.silverPrevClose,
        silverThirdLastClose: latestRates.silverThirdLastClose,
      });
      return true;
    }
  } catch (error) {
    console.log("Historical close refresh failed:", error.message);
  }
  return false;
}

async function fetchLastAvailableQuotes() {
  await ensureValidAccessToken();
  if (!accessToken || !GOLD_KEY || !SILVER_KEY) return false;

  try {
    const instrumentKeys = encodeURIComponent(`${GOLD_KEY},${SILVER_KEY}`);
    const url = `https://api.upstox.com/v2/market-quote/quotes?instrument_key=${instrumentKeys}`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
      timeout: 5000,
    });

    const goldQuote = pickQuoteObject(response.data, GOLD_KEY);
    const silverQuote = pickQuoteObject(response.data, SILVER_KEY);
    const updatedGold = applyQuoteFallback(goldQuote, "gold");
    const updatedSilver = applyQuoteFallback(silverQuote, "silver");

    if (updatedGold || updatedSilver) {
      refreshMarketClosedState();
      latestRates.lastUpdated = new Date().toISOString();
      latestRates.source = "upstox-last-quote";
      latestRates.status = "Showing last available Upstox quote. Live MCX will update automatically when market opens.";
      await refreshHistoricalTradingCloses(false);
      saveCachedRates();
      recordLastGoodRatesToRender("rest-last-quote");
      broadcast();
      console.log("Last available quotes updated from Upstox REST API.");
      return true;
    }
  } catch (error) {
    const msg = error.response?.data?.errors?.[0]?.message || error.response?.data?.message || error.message;
    console.log("Last quote fallback failed:", error.response?.data || error.message);
    if (String(msg).toLowerCase().includes("token") || error.response?.status === 401 || error.response?.status === 403) {
      if (tryMemoryTokenFallback()) return fetchLastAvailableQuotes();
      const refreshed = await refreshAccessTokenIfPossible(true);
      if (refreshed) return fetchLastAvailableQuotes();
      markTokenReconnectNeeded("Upstox access token expired/invalid and auto refresh is unavailable. Open /upstox once.");
      broadcast();
    }
  }

  return false;
}

async function getAuthorizedWebSocketUrl() {
  await ensureValidAccessToken();
  try {
    const response = await axios.get(UPSTOX_AUTHORIZE_URL, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
      },
    });

    return response.data.data.authorized_redirect_uri;
  } catch (error) {
    if (error.response?.status === 401 || error.response?.status === 403) {
      if (tryMemoryTokenFallback()) {
        const response = await axios.get(UPSTOX_AUTHORIZE_URL, {
          headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
        });
        return response.data.data.authorized_redirect_uri;
      }
      const refreshed = await refreshAccessTokenIfPossible(true);
      if (refreshed) {
        const response = await axios.get(UPSTOX_AUTHORIZE_URL, {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            Accept: "application/json",
          },
        });
        return response.data.data.authorized_redirect_uri;
      }
    }
    throw error;
  }
}

async function connectUpstox() {
  if (upstoxWsConnecting) return;
  upstoxWsConnecting = true;
  if (!accessToken || !GOLD_KEY || !SILVER_KEY) {
    latestRates.status = "Missing access token or instrument keys. Manual mode available.";
    latestRates.source = "manual";
    console.log(latestRates.status);
    upstoxWsConnecting = false;
    return;
  }

  let authorizedUrl;
  try {
    authorizedUrl = await getAuthorizedWebSocketUrl();
  } catch (error) {
    const msg = error.response?.data?.errors?.[0]?.message || error.response?.data?.message || error.message;
    if (String(msg).toLowerCase().includes("token") || error.response?.status === 401 || error.response?.status === 403) {
      markTokenReconnectNeeded("Upstox access token expired/invalid and auto refresh is unavailable. Open /upstox once.");
    } else {
      latestRates.status = `WebSocket authorize error: ${msg}`;
      latestRates.source = "error";
    }
    console.error(latestRates.status);
    broadcast();
    upstoxWsConnecting = false;
    liveFeedStatus = "Reconnect scheduled";
    setTimeout(connectUpstox, 30000);
    return;
  }

  if (currentUpstoxWs && currentUpstoxWs.readyState === WebSocket.OPEN) {
    try { currentUpstoxWs.close(); } catch {}
  }

  upstoxWsConnecting = false;
  const ws = new WebSocket(authorizedUrl, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });
  currentUpstoxWs = ws;

  ws.on("open", () => {
    upstoxWsAlive = true;
    lastWebSocketPongAt = new Date().toISOString();
    liveFeedStatus = "Connected";
    latestRates.status = "Connected to Upstox WebSocket.";
    latestRates.source = "upstox";
    console.log(latestRates.status);

    const subRequest = {
      guid: `bullion-live-${Date.now()}`,
      method: "sub",
      data: {
        mode: "full_d5",
        instrumentKeys: [GOLD_KEY, SILVER_KEY],
      },
    };

    // Upstox V3 subscription request must be sent as binary data.
    ws.send(Buffer.from(JSON.stringify(subRequest)));
  });

  ws.on("message", (buffer) => {
    lastWebSocketMessageAt = new Date().toISOString();
    upstoxWsAlive = true;
    liveFeedStatus = "Receiving data";
    try {
      let data;

      try {
        // Some control messages may arrive as JSON/text.
        data = JSON.parse(buffer.toString());
      } catch {
        // Upstox V3 live feed arrives as protobuf binary.
        data = decodeUpstoxBinaryFeed(buffer);
      }

      const gold = extractLtpFromFeed(data, GOLD_KEY);
      const silver = extractLtpFromFeed(data, SILVER_KEY);
      const goldOhlc = extractDayOhlcFromFeed(data, GOLD_KEY);
      const silverOhlc = extractDayOhlcFromFeed(data, SILVER_KEY);

      clearMarketClosedIfRateChanged(gold ?? latestRates.goldMcx, silver ?? latestRates.silverMcx);
      if (gold != null) latestRates.goldMcx = gold;
      if (silver != null) latestRates.silverMcx = silver;
      if (goldOhlc) {
        if (goldOhlc.open != null) latestRates.goldOpen = goldOhlc.open;
        // Do not store gold prev/third close from websocket day close; use historical daily candles only.
        if (goldOhlc.high != null) latestRates.goldHigh = goldOhlc.high;
        if (goldOhlc.low != null) latestRates.goldLow = goldOhlc.low;
      }
      if (silverOhlc) {
        if (silverOhlc.open != null) latestRates.silverOpen = silverOhlc.open;
        // Do not store silver prev/third close from websocket day close; use historical daily candles only.
        if (silverOhlc.high != null) latestRates.silverHigh = silverOhlc.high;
        if (silverOhlc.low != null) latestRates.silverLow = silverOhlc.low;
      }

      if (gold != null || silver != null || goldOhlc || silverOhlc) {
        refreshMarketClosedState();
        latestRates.lastUpdated = new Date().toISOString();
        latestRates.source = "upstox";
        latestRates.status = "Live rates updated.";
        console.log("Live rates updated:", {
          gold: latestRates.goldMcx,
          silver: latestRates.silverMcx,
        });
        refreshHistoricalTradingCloses(false).catch(() => {});
        saveCachedRates();
        recordLastGoodRatesToRender("websocket-live");
        broadcast();
      } else if (data?.type === "market_info") {
        latestRates.status = "Market info received. Waiting for live prices.";
        fetchLastAvailableQuotes();
        broadcast();
      }
    } catch (error) {
      latestRates.status = `Feed parse error: ${error.message}`;
      console.error(error);
      broadcast();
    }
  });

  ws.on("error", (error) => {
    liveFeedStatus = "Error";
    latestRates.status = `Upstox WebSocket error: ${error.message}`;
    latestRates.source = "error";
    console.error(latestRates.status);
    broadcast();
  });

  ws.on("pong", () => {
    upstoxWsAlive = true;
    lastWebSocketPongAt = new Date().toISOString();
    liveFeedStatus = "Connected";
  });

  ws.on("close", () => {
    upstoxWsAlive = false;
    upstoxWsConnecting = false;
    liveFeedStatus = "Reconnecting";
    websocketReconnectCount += 1;
    latestRates.status = "Upstox WebSocket closed. Reconnecting...";
    latestRates.source = "reconnecting";
    console.log(latestRates.status);
    broadcast();
    setTimeout(connectUpstox, 5000);
  });
}



function monitorUpstoxHeartbeat() {
  lastHeartbeatCheckAt = new Date().toISOString();
  refreshMarketClosedState();
  if (!currentUpstoxWs) {
    liveFeedStatus = "Not connected";
    setTimeout(connectUpstox, 1000);
    return;
  }

  if (currentUpstoxWs.readyState === WebSocket.OPEN) {
    const lastPongMs = lastWebSocketPongAt ? new Date(lastWebSocketPongAt).getTime() : 0;
    const pongAge = lastPongMs ? Date.now() - lastPongMs : Infinity;
    if (!upstoxWsAlive || pongAge > 45000) {
      liveFeedStatus = "Frozen - reconnecting";
      latestRates.status = "Upstox WebSocket heartbeat failed. Reconnecting live feed...";
      try { currentUpstoxWs.terminate(); } catch {}
      currentUpstoxWs = null;
      websocketReconnectCount += 1;
      broadcast();
      setTimeout(connectUpstox, 1000);
      return;
    }
    upstoxWsAlive = false;
    try { currentUpstoxWs.ping(); } catch (error) {
      liveFeedStatus = "Ping failed - reconnecting";
      try { currentUpstoxWs.terminate(); } catch {}
      currentUpstoxWs = null;
      websocketReconnectCount += 1;
      setTimeout(connectUpstox, 1000);
    }
  } else if (currentUpstoxWs.readyState === WebSocket.CLOSED || currentUpstoxWs.readyState === WebSocket.CLOSING) {
    liveFeedStatus = "Closed - reconnecting";
    websocketReconnectCount += 1;
    setTimeout(connectUpstox, 1000);
  } else {
    liveFeedStatus = "Connecting";
  }
}

app.get("/upstox", (req, res) => {
  const ready = Boolean(UPSTOX_API_KEY && UPSTOX_API_SECRET);
  const loginUrl = ready ? getUpstoxLoginUrl(req) : null;
  res.send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Reconnect Upstox</title><style>body{font-family:Arial,sans-serif;background:#111;color:#fff;padding:24px;line-height:1.45}.card{max-width:720px;margin:auto;background:#1d1d1d;border:1px solid #444;border-radius:18px;padding:24px}a.btn{display:inline-block;background:#d4af37;color:#111;padding:13px 18px;border-radius:12px;text-decoration:none;font-weight:700}.warn{color:#ffd36a}.ok{color:#74ff8a}code{background:#000;padding:2px 5px;border-radius:5px}</style></head><body><div class="card"><h1>R K Jewellers - Upstox Reconnect</h1><p>Status: <b>${tokenNeedsReconnect ? '<span class="warn">Reconnect required</span>' : '<span class="ok">Token present</span>'}</b></p><p>${tokenLastError || latestRates.status || ''}</p>${ready ? `<p><a class="btn" href="${loginUrl}">Reconnect Upstox Now</a></p>` : `<p class="warn">Missing environment variables. Add <code>UPSTOX_API_KEY</code>, <code>UPSTOX_API_SECRET</code>, and optionally <code>UPSTOX_REDIRECT_URI</code> in Render.</p>`}<p>Redirect URI to add in Upstox app settings:</p><p><code>${getRedirectUri(req)}</code></p><p>After reconnect, the backend will save the new access token and expiry time in MongoDB/server storage. Upstox reconnect is still available because refresh-token auto renewal is not supported in this setup.</p><p>After reconnect, open <code>/api/rates</code> again.</p></div></body></html>`);
});

async function updateRenderEnvironmentVariable(envKey, envValue) {
  const apiKey = process.env.RENDER_API_KEY || process.env.RENDER_TOKEN || "";
  const serviceId = process.env.RENDER_SERVICE_ID || "";
  if (!apiKey || !serviceId) {
    return { updated: false, reason: "RENDER_API_KEY or RENDER_SERVICE_ID missing. Value saved in MongoDB/server memory only." };
  }

  const headers = {
    Authorization: `Bearer ${apiKey}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  const baseUrl = `https://api.render.com/v1/services/${serviceId}/env-vars`;
  const value = String(envValue ?? "");

  const attempts = [];
  try {
    const list = await axios.get(baseUrl, { headers, timeout: 15000 });
    const rows = Array.isArray(list.data) ? list.data : (Array.isArray(list.data?.envVars) ? list.data.envVars : []);
    const found = rows.find(x =>
      x?.envVar?.key === envKey ||
      x?.key === envKey ||
      x?.envVarKey === envKey ||
      x?.name === envKey
    );
    const id = found?.envVar?.id || found?.id || found?.envVarId || null;

    if (id) {
      const patchBodies = [
        { value },
        { envVar: { value } },
        { key: envKey, value },
        { envVar: { key: envKey, value } },
      ];
      for (const body of patchBodies) {
        try {
          await axios.patch(`${baseUrl}/${id}`, body, { headers, timeout: 15000 });
          process.env[envKey] = value;
          return { updated: true, method: "patch-by-id" };
        } catch (err) {
          attempts.push(`patch-by-id: ${err.response?.status || err.message}`);
        }
      }
    }

    // Some Render API versions identify env vars by key instead of id.
    for (const method of ["patch", "put"]) {
      for (const body of [{ value }, { key: envKey, value }, { envVar: { key: envKey, value } }]) {
        try {
          await axios({ method, url: `${baseUrl}/${encodeURIComponent(envKey)}`, data: body, headers, timeout: 15000 });
          process.env[envKey] = value;
          return { updated: true, method: `${method}-by-key` };
        } catch (err) {
          attempts.push(`${method}-by-key: ${err.response?.status || err.message}`);
        }
      }
    }

    // Create if not found / if update endpoints fail.
    try {
      await axios.post(baseUrl, { key: envKey, value }, { headers, timeout: 15000 });
      process.env[envKey] = value;
      return { updated: true, method: "post-create" };
    } catch (err) {
      attempts.push(`post-create: ${err.response?.status || err.message}`);
    }

    return { updated: false, reason: attempts.join(" | ") || "Render API did not accept update." };
  } catch (error) {
    return { updated: false, reason: error.response?.data || error.message };
  }
}

async function updateRenderAccessTokenEnv(newToken) {
  const envKey = process.env.RENDER_ACCESS_TOKEN_ENV_KEY || "UPSTOX_ACCESS_TOKEN";
  return updateRenderEnvironmentVariable(envKey, newToken);
}



app.post("/api/admin/login", (req, res) => {
  const password = String(req.body?.password || "");
  const purpose = String(req.body?.purpose || "admin");
  const isAccessPassword = password === ADMIN_ACCESS_PASSWORD;
  const isUpdatePassword = password === ADMIN_UPDATE_PASSWORD;
  if (!isAccessPassword && !isUpdatePassword) {
    return res.status(401).json({ ok: false, message: "Wrong Password" });
  }
  const permissions = isUpdatePassword
    ? ["atu:access", "token:view", "token:update", "upstox:reconnect", "settings:update"]
    : ["atu:access"];
  const token = signAdminJwt({ role: "admin", purpose, permissions });
  const payload = verifyAdminJwt(token);
  rememberAdminSession(token, payload);
  res.json({ ok: true, token, expiresInSeconds: JWT_EXPIRY_SECONDS, permissions, jwtEnabled:true, adminSession:true, authVerified:true, adminSessionExpiresAt: getAdminSessionState(req).adminSessionExpiresAt });
});


app.get("/api/admin/session", requireAdminJwt, (req, res) => {
  res.json({ ok: true, ...getAdminSessionState(req), admin: req.admin });
});

app.post("/api/admin/upstox-login-url", requireAdminJwt, (req, res) => {
  const permissions = req.admin?.permissions || [];
  if (!permissions.includes("upstox:reconnect")) {
    return res.status(403).json({ ok: false, message: "Reconnect permission denied." });
  }
  if (!UPSTOX_API_KEY) return res.status(500).json({ ok: false, message: "Missing UPSTOX_API_KEY in Render Environment." });
  res.json({ ok: true, loginUrl: getUpstoxLoginUrl(req) });
});

app.post("/api/upstox/manual-token", requireAdminJwt, async (req, res) => {
  const token = String(req.body?.accessToken || "").trim();
  if (!token) return res.status(400).json({ ok: false, message: "Access token is blank." });
  await saveAccessToken({ access_token: token, source: "manual", generatedBy: "manual" });
  latestRates.status = "Access token manually updated from ATU page.";
  latestRates.source = "atu-token-update";
  const renderUpdate = await updateRenderAccessTokenEnv(token);
  markRenderTokenActiveIfPossible(renderUpdate);
  try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_EXPIRES_AT", accessTokenExpiresAt || ""); } catch {}
  try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_UPDATED_AT", accessTokenUpdatedAt || ""); } catch {}
  try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_GENERATED_BY", accessTokenGeneratedBy || ""); } catch {}
  try { await fetchLastAvailableQuotes(); } catch {}
  try { if (currentUpstoxWs) currentUpstoxWs.close(); } catch {}
  setTimeout(connectUpstox, 1000);
  broadcast();
  res.json({ ok: true, savedToMongoDB: Boolean(tokenCollection), savedToServerFile: true, renderEnvironment: renderUpdate, accessTokenExpiresAt });
});


app.get("/api/upstox/current-token", requireAdminJwt, (req, res) => {
  res.json({
    ok: Boolean(accessToken),
    accessToken: accessToken || "",
    updatedAt: accessTokenUpdatedAt || accessTokenExpiresAt || null,
    generatedBy: accessTokenGeneratedBy || null,
    tokenGeneratedBy: accessTokenGeneratedBy || null,
    accessTokenExpiresAt,
    tokenStorage: activeTokenSource,
    mongoConnected: Boolean(tokenCollection),
    mongoWarning: getMongoFallbackMessage(),
  });
});

app.get("/api/upstox/status", (req, res) => {
  const adminState = getAdminSessionState(req);
  res.json({
    hasApiKey: Boolean(UPSTOX_API_KEY),
    hasApiSecret: Boolean(UPSTOX_API_SECRET),
    hasAccessToken: Boolean(accessToken),
    tokenNeedsReconnect,
    tokenLastError,
    loginUrl: UPSTOX_API_KEY ? getUpstoxLoginUrl(req) : null,
    redirectUri: getRedirectUri(req),
    tokenStorage: activeTokenSource,
    mongoConnected: Boolean(tokenCollection),
    mongoWarning: getMongoFallbackMessage(),
    mongoLastError,
    usingRenderBackupToken: activeTokenSource === "render-env-backup",
    usingMemoryBackupToken: activeTokenSource === "server-memory-backup",
    refreshTokenPresent: Boolean(refreshToken),
    tokenAutoRefreshEnabled,
    accessTokenExpiresAt,
    tokenGeneratedBy: accessTokenGeneratedBy || null,
    tokenExpiryDisplay: tokenExpiryState().label,
    tokenExpired: tokenExpiryState().expired,
    tokenWorking: tokensWorking(),
    renderTokenAccess: activeTokenSource === "render-env-backup" && tokensWorking(),
    backendMemoryTokenAccess: activeTokenSource === "server-memory-backup" && tokensWorking(),
    jwtEnabled: adminState.jwtEnabled,
    adminSession: adminState.adminSession,
    authVerified: adminState.authVerified,
    adminSessionExpiresAt: adminState.adminSessionExpiresAt,
    lastAutoRefreshAt,
    liveFeedStatus,
    lastWebSocketMessageAt,
    lastWebSocketPongAt,
    websocketReconnectCount,
    lastHeartbeatCheckAt,
    status: latestRates.status,
  });
});

app.get("/api/upstox/login", (req, res) => {
  if (!UPSTOX_API_KEY) return res.status(500).send("Missing UPSTOX_API_KEY in Render Environment.");
  res.redirect(getUpstoxLoginUrl(req));
});

app.get("/api/upstox/callback", async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send("Missing authorization code from Upstox.");
  if (!UPSTOX_API_KEY || !UPSTOX_API_SECRET) return res.status(500).send("Missing UPSTOX_API_KEY or UPSTOX_API_SECRET in Render Environment.");

  try {
    const form = new URLSearchParams({
      code: String(code),
      client_id: UPSTOX_API_KEY,
      client_secret: UPSTOX_API_SECRET,
      redirect_uri: getRedirectUri(req),
      grant_type: "authorization_code",
    });

    const response = await axios.post(UPSTOX_TOKEN_URL, form.toString(), {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
      },
      timeout: 15000,
    });

    await saveAccessToken({ ...response.data, source: "reconnect-to-upstox", generatedBy: "reconnect-to-upstox" });
    const newTokenFromReconnect = response.data?.access_token || response.data?.accessToken;
    if (newTokenFromReconnect) {
      const renderUpdate = await updateRenderAccessTokenEnv(newTokenFromReconnect);
      markRenderTokenActiveIfPossible(renderUpdate);
      try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_EXPIRES_AT", accessTokenExpiresAt || ""); } catch {}
      try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_UPDATED_AT", accessTokenUpdatedAt || ""); } catch {}
      try { await updateRenderEnvironmentVariable("UPSTOX_TOKEN_GENERATED_BY", accessTokenGeneratedBy || ""); } catch {}
    }
    latestRates.status = "Upstox reconnected successfully. Fetching latest rates.";
    latestRates.source = "upstox-reconnected";
    await fetchLastAvailableQuotes();
    setTimeout(connectUpstox, 1000);

    res.send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Upstox Connected</title><style>body{font-family:Arial,sans-serif;background:#111;color:#fff;padding:24px}.card{max-width:650px;margin:auto;background:#1d1d1d;border-radius:18px;padding:24px}.ok{color:#74ff8a}a{color:#ffd36a}</style></head><body><div class="card"><h1 class="ok">TOKEN UPDATED SUCCESSFULLY</h1><p>You can close this page now.</p><p><a href="/api/rates">Check live rates</a></p></div></body></html>`);
  } catch (error) {
    const details = JSON.stringify(error.response?.data || error.message);
    console.error("Upstox token exchange failed:", details);
    res.status(500).send(`Upstox token exchange failed: ${details}`);
  }
});

app.get("/", (req, res) => {
  res.json({ message: "R K Jewellers backend is running", rates: "/api/rates", upstoxReconnect: "/upstox" });
});

app.get("/rates", (req, res) => {
  res.redirect("/api/rates");
});

app.get("/api/rates", async (req, res) => {
  if (!GOLD_KEY || !SILVER_KEY) {
    await prepareInstrumentKeys();
  }
  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx, req));
});

app.post("/api/manual-rates", requireAdminJwt, (req, res) => {
  const gold = Number(req.body.goldMcx);
  const silver = Number(req.body.silverMcx);
  const goldOpen = Number(req.body.goldOpen);
  const silverOpen = Number(req.body.silverOpen);
  const goldHigh = Number(req.body.goldHigh);
  const silverHigh = Number(req.body.silverHigh);
  const goldLow = Number(req.body.goldLow);
  const silverLow = Number(req.body.silverLow);

  if (Number.isFinite(gold)) latestRates.goldMcx = gold;
  if (Number.isFinite(silver)) latestRates.silverMcx = silver;
  if (Number.isFinite(goldOpen)) latestRates.goldOpen = goldOpen;
  if (Number.isFinite(silverOpen)) latestRates.silverOpen = silverOpen;
  if (Number.isFinite(goldHigh)) latestRates.goldHigh = goldHigh;
  if (Number.isFinite(silverHigh)) latestRates.silverHigh = silverHigh;
  if (Number.isFinite(goldLow)) latestRates.goldLow = goldLow;
  if (Number.isFinite(silverLow)) latestRates.silverLow = silverLow;

  latestRates.lastUpdated = new Date().toISOString();
  latestRates.source = "manual";
  latestRates.status = "Manual rates saved.";
  saveCachedRates();
  recordLastGoodRatesToRender("manual-rates");
  broadcast();

  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

app.post("/api/rate-difference", requireAdminJwt, async (req, res) => {
  const permissions = req.admin?.permissions || [];
  if (!permissions.includes("settings:update")) return res.status(403).json({ ok:false, message:"Settings update permission denied." });

  const hasGoldDifference = Object.prototype.hasOwnProperty.call(req.body, "goldDifference");
  const hasSilverDifference = Object.prototype.hasOwnProperty.call(req.body, "silverDifference");
  const hasGoldPhysical = Object.prototype.hasOwnProperty.call(req.body, "goldPhysicalRate");
  const hasSilverPhysical = Object.prototype.hasOwnProperty.call(req.body, "silverPhysicalRate");

  const nowIso = new Date().toISOString();
  const renderUpdates = {};

  if (hasGoldDifference || hasGoldPhysical) {
    const currentGoldMcx = Number(latestRates.goldMcx);
    let finalGoldDiff = null;

    if (hasGoldPhysical) {
      const physicalGoldRate = Number(req.body.goldPhysicalRate);
      if (!Number.isFinite(physicalGoldRate) || physicalGoldRate < 0) {
        return res.status(400).json({ ok:false, message:"Physical gold rate must be zero or positive." });
      }
      if (!Number.isFinite(currentGoldMcx)) {
        return res.status(400).json({ ok:false, message:"Gold MCX rate not available. Please wait for live rate and try again." });
      }
      finalGoldDiff = physicalGoldRate - currentGoldMcx;
    } else {
      finalGoldDiff = Number(req.body.goldDifference);
      if (!Number.isFinite(finalGoldDiff)) {
        return res.status(400).json({ ok:false, message:"Invalid gold difference." });
      }
    }

    goldDifference = finalGoldDiff;
    goldDifferenceUpdatedAt = nowIso;
    goldDifferenceMcxAtUpdate = Number.isFinite(currentGoldMcx) ? currentGoldMcx : null;

    renderUpdates.goldDifference = await updateRenderEnvironmentVariable("GOLD_RATE_DIFFERENCE", String(finalGoldDiff));
    renderUpdates.goldDifferenceUpdatedAt = await updateRenderEnvironmentVariable("GOLD_RATE_DIFFERENCE_UPDATED_AT", nowIso);
    if (goldDifferenceMcxAtUpdate != null) {
      renderUpdates.goldDifferenceMcxAtUpdate = await updateRenderEnvironmentVariable("GOLD_RATE_DIFFERENCE_MCX_AT_UPDATE", String(goldDifferenceMcxAtUpdate));
    }

    if (tokenCollection) {
      try {
        await tokenCollection.updateOne({ _id: TOKEN_DOC_ID }, { $set: {
          goldDifference,
          goldDifferenceUpdatedAt,
          goldDifferenceMcxAtUpdate,
          goldPhysicalRateAtUpdate: hasGoldPhysical ? Number(req.body.goldPhysicalRate) : null,
        } }, { upsert: true });
      } catch (e) { console.log("Could not save gold difference metadata to MongoDB:", e.message); }
    }
  }

  if (hasSilverDifference || hasSilverPhysical) {
    const currentSilverMcx = Number(latestRates.silverMcx);
    let finalSilverDiff = null;

    if (hasSilverPhysical) {
      const physicalSilverRate = Number(req.body.silverPhysicalRate);
      if (!Number.isFinite(physicalSilverRate) || physicalSilverRate < 0) {
        return res.status(400).json({ ok:false, message:"Physical silver rate must be zero or positive." });
      }
      if (!Number.isFinite(currentSilverMcx)) {
        return res.status(400).json({ ok:false, message:"Silver MCX rate not available. Please wait for live rate and try again." });
      }
      finalSilverDiff = physicalSilverRate - currentSilverMcx;
    } else {
      finalSilverDiff = Number(req.body.silverDifference);
      if (!Number.isFinite(finalSilverDiff)) {
        return res.status(400).json({ ok:false, message:"Invalid silver difference." });
      }
    }

    silverDifference = finalSilverDiff;
    silverDifferenceUpdatedAt = nowIso;
    silverDifferenceMcxAtUpdate = Number.isFinite(currentSilverMcx) ? currentSilverMcx : null;

    renderUpdates.silverDifference = await updateRenderEnvironmentVariable("SILVER_RATE_DIFFERENCE", String(finalSilverDiff));
    renderUpdates.silverDifferenceUpdatedAt = await updateRenderEnvironmentVariable("SILVER_RATE_DIFFERENCE_UPDATED_AT", nowIso);
    if (silverDifferenceMcxAtUpdate != null) {
      renderUpdates.silverDifferenceMcxAtUpdate = await updateRenderEnvironmentVariable("SILVER_RATE_DIFFERENCE_MCX_AT_UPDATE", String(silverDifferenceMcxAtUpdate));
    }

    if (tokenCollection) {
      try {
        await tokenCollection.updateOne({ _id: TOKEN_DOC_ID }, { $set: {
          silverDifference,
          silverDifferenceUpdatedAt,
          silverDifferenceMcxAtUpdate,
          silverPhysicalRateAtUpdate: hasSilverPhysical ? Number(req.body.silverPhysicalRate) : null,
        } }, { upsert: true });
      } catch (e) { console.log("Could not save silver difference metadata to MongoDB:", e.message); }
    }
  }

  latestRates.lastUpdated = nowIso;
  latestRates.status = "Metal rate difference updated.";
  saveCachedRates();
  broadcast();

  const result = calculateRates(latestRates.goldMcx, latestRates.silverMcx, req);
  result.ok = true;
  result.renderEnvironmentUpdates = renderUpdates;
  res.json(result);
});

async function startServer() {
  await initMongoTokenStore();
  await loadSavedAccessToken();
  await prepareInstrumentKeys();
  refreshHistoricalTradingCloses(true).catch(() => {});
  fetchLastAvailableQuotes();
  setInterval(fetchLastAvailableQuotes, 1000);
  setInterval(() => refreshHistoricalTradingCloses(false).catch(() => {}), 10 * 60 * 1000);
  setInterval(monitorUpstoxHeartbeat, 20000);
  setInterval(async () => {
    await prepareInstrumentKeys();
    await fetchLastAvailableQuotes();
    try { if (currentUpstoxWs) currentUpstoxWs.close(); } catch {}
    setTimeout(connectUpstox, 1000);
  }, 15 * 60 * 1000);
  const httpServer = app.listen(PORT, () => {
    console.log(`Backend running at http://localhost:${PORT}`);
    connectUpstox();
  });

  const wss = new WebSocket.Server({ server: httpServer, path: "/live" });

  wss.on("connection", (client) => {
    clients.add(client);
    client.send(JSON.stringify({
      type: "rates",
      data: calculateRates(latestRates.goldMcx, latestRates.silverMcx),
    }));

    client.on("close", () => clients.delete(client));
  });
}

startServer().catch((error) => {
  console.error("Server start failed:", error);
});
