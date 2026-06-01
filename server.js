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

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 4000;
const UPSTOX_AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize";
const UPSTOX_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token";
const renderEnvAccessToken = process.env.UPSTOX_ACCESS_TOKEN || process.env.UPSTOX_accessToken || null;
let accessToken = renderEnvAccessToken || null;
let activeTokenSource = renderEnvAccessToken ? "render-env-backup" : "none";
let memoryBackupAccessToken = renderEnvAccessToken || null;
let refreshToken = null; // Upstox does not provide refresh-token auto renewal in this setup.
let accessTokenExpiresAt = process.env.UPSTOX_TOKEN_EXPIRES_AT || null;
let accessTokenUpdatedAt = process.env.UPSTOX_TOKEN_UPDATED_AT || null;
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

let tokenNeedsReconnect = false;
let tokenLastError = null;
let currentUpstoxWs = null;
let tokenAutoRefreshEnabled = false; // intentionally disabled: Upstox refresh-token auto renewal is not supported here.

// Metal rate difference from .env. You can keep blank/0 and control from frontend/API.
let goldDifference = Number(process.env.GOLD_RATE_DIFFERENCE || 0);
let silverDifference = Number(process.env.SILVER_RATE_DIFFERENCE || 0);
let goldDifferenceUpdatedAt = process.env.GOLD_RATE_DIFFERENCE_UPDATED_AT || null;
let silverDifferenceUpdatedAt = process.env.SILVER_RATE_DIFFERENCE_UPDATED_AT || null;

let GOLD_KEY = null;
let SILVER_KEY = null;

let latestRates = {
  goldMcx: null,
  silverMcx: null,
  goldOpen: null,
  silverOpen: null,
  goldHigh: null,
  silverHigh: null,
  goldLow: null,
  silverLow: null,
  lastUpdated: null,
  source: "waiting",
  status: "Server started. Waiting for Upstox feed.",
};

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
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 10000 });
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

  if (savedAccess) {
    accessToken = savedAccess;
    memoryBackupAccessToken = savedAccess;
    activeTokenSource = sourceLabel === "MongoDB" ? "mongodb" : (sourceLabel === "Render environment" ? "render-env-backup" : "server-memory-backup");
  }
  if (savedRefresh) refreshToken = savedRefresh;
  if (savedExpiresAt) accessTokenExpiresAt = savedExpiresAt;
  if (savedUpdatedAt) accessTokenUpdatedAt = savedUpdatedAt;

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

function buildTokenExpiry(tokenData) {
  const rawExpiresAt = tokenData.expires_at || tokenData.expiresAt;
  if (rawExpiresAt) return new Date(rawExpiresAt).toISOString();

  const expiresInSeconds = Number(tokenData.expires_in || tokenData.expiresIn);
  if (Number.isFinite(expiresInSeconds) && expiresInSeconds > 0) {
    return new Date(Date.now() + expiresInSeconds * 1000).toISOString();
  }

  // Safe fallback: many broker access tokens are short lived. Refresh once per day if no expiry is returned.
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

  const dataToSave = {
    ...tokenData,
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_at: accessTokenExpiresAt,
    autoRefreshEnabled: tokenAutoRefreshEnabled,
    savedAt: accessTokenUpdatedAt,
    updatedAt: accessTokenUpdatedAt,
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
  tokenLastError = message || "Upstox token expired or invalid. Reconnect Upstox once.";
  latestRates.source = latestRates.goldMcx || latestRates.silverMcx ? "upstox-last-quote" : "token-expired";
  latestRates.status = tokenLastError;
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

function calculateRates(goldMcx, silverMcx) {
  const gold = Number(goldMcx);
  const silver = Number(silverMcx);
  const gDiff = Number(goldDifference || 0);
  const sDiff = Number(silverDifference || 0);

  // Formula 1: 24K = (MCX Gold Rate + Gold Rate Difference) / 0.995
  const gold24k = Number.isFinite(gold) ? (gold + gDiff) / 0.995 : null;

  // Formula 2: Silver 1KG = MCX Silver Rate + Silver Rate Difference
  const silver1kg = Number.isFinite(silver) ? silver + sDiff : null;

  return {
    goldMcx: Number.isFinite(gold) ? gold : null,
    silverMcx: Number.isFinite(silver) ? silver : null,
    goldOpen: latestRates.goldOpen,
    silverOpen: latestRates.silverOpen,
    goldHigh: latestRates.goldHigh,
    silverHigh: latestRates.silverHigh,
    goldLow: latestRates.goldLow,
    silverLow: latestRates.silverLow,

    goldDifference: gDiff,
    silverDifference: sDiff,
    goldDifferenceUpdatedAt,
    silverDifferenceUpdatedAt,

    gold24k,
    gold22k: gold24k != null ? gold24k * 0.916 : null,
    gold20k: gold24k != null ? gold24k * 0.8334 : null,
    gold18k: gold24k != null ? gold24k * 0.75 : null,
    gold14k: gold24k != null ? gold24k * 0.5834 : null,

    silver1kg,
    silver10gram: silver1kg != null ? silver1kg / 100 : null,

    lastUpdated: latestRates.lastUpdated,
    source: latestRates.source,
    status: latestRates.status,
    tokenNeedsReconnect,
    tokenLastError,
    tokenAutoRefreshEnabled,
    refreshTokenPresent: Boolean(refreshToken),
    accessTokenExpiresAt,
    lastAutoRefreshAt,
    reconnectPath: tokenNeedsReconnect ? "/upstox" : null,
    mongoConnected: Boolean(tokenCollection),
    tokenStorage: activeTokenSource,
    usingRenderBackupToken: activeTokenSource === "render-env-backup",
    usingMemoryBackupToken: activeTokenSource === "server-memory-backup",
    mongoWarning: getMongoFallbackMessage(),
    mongoLastError,
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
  return {
    open: Number.isFinite(open) ? open : null,
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
  const high = Number(ohlc.high ?? quote.high);
  const low = Number(ohlc.low ?? quote.low);

  let updated = false;
  if (Number.isFinite(ltp)) { latestRates[`${metal}Mcx`] = ltp; updated = true; }
  if (Number.isFinite(open)) { latestRates[`${metal}Open`] = open; updated = true; }
  if (Number.isFinite(high)) { latestRates[`${metal}High`] = high; updated = true; }
  if (Number.isFinite(low)) { latestRates[`${metal}Low`] = low; updated = true; }
  return updated;
}

async function fetchLastAvailableQuotes() {
  await ensureValidAccessToken();
  if (!accessToken || !GOLD_KEY || !SILVER_KEY) return false;

  try {
    const instrumentKeys = encodeURIComponent(`${GOLD_KEY},${SILVER_KEY}`);
    const url = `https://api.upstox.com/v2/market-quote/quotes?instrument_key=${instrumentKeys}`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
      timeout: 10000,
    });

    const goldQuote = pickQuoteObject(response.data, GOLD_KEY);
    const silverQuote = pickQuoteObject(response.data, SILVER_KEY);
    const updatedGold = applyQuoteFallback(goldQuote, "gold");
    const updatedSilver = applyQuoteFallback(silverQuote, "silver");

    if (updatedGold || updatedSilver) {
      latestRates.lastUpdated = new Date().toISOString();
      latestRates.source = "upstox-last-quote";
      latestRates.status = "Showing last available Upstox quote. Live MCX will update automatically when market opens.";
      saveCachedRates();
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
  if (!accessToken || !GOLD_KEY || !SILVER_KEY) {
    latestRates.status = "Missing access token or instrument keys. Manual mode available.";
    latestRates.source = "manual";
    console.log(latestRates.status);
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
    setTimeout(connectUpstox, 30000);
    return;
  }

  if (currentUpstoxWs && currentUpstoxWs.readyState === WebSocket.OPEN) {
    try { currentUpstoxWs.close(); } catch {}
  }

  const ws = new WebSocket(authorizedUrl, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });
  currentUpstoxWs = ws;

  ws.on("open", () => {
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

      if (gold != null) latestRates.goldMcx = gold;
      if (silver != null) latestRates.silverMcx = silver;
      if (goldOhlc) {
        if (goldOhlc.open != null) latestRates.goldOpen = goldOhlc.open;
        if (goldOhlc.high != null) latestRates.goldHigh = goldOhlc.high;
        if (goldOhlc.low != null) latestRates.goldLow = goldOhlc.low;
      }
      if (silverOhlc) {
        if (silverOhlc.open != null) latestRates.silverOpen = silverOhlc.open;
        if (silverOhlc.high != null) latestRates.silverHigh = silverOhlc.high;
        if (silverOhlc.low != null) latestRates.silverLow = silverOhlc.low;
      }

      if (gold != null || silver != null || goldOhlc || silverOhlc) {
        latestRates.lastUpdated = new Date().toISOString();
        latestRates.source = "upstox";
        latestRates.status = "Live rates updated.";
        console.log("Live rates updated:", {
          gold: latestRates.goldMcx,
          silver: latestRates.silverMcx,
        });
        saveCachedRates();
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
    latestRates.status = `Upstox WebSocket error: ${error.message}`;
    latestRates.source = "error";
    console.error(latestRates.status);
    broadcast();
  });

  ws.on("close", () => {
    latestRates.status = "Upstox WebSocket closed. Reconnecting...";
    latestRates.source = "reconnecting";
    console.log(latestRates.status);
    broadcast();
    setTimeout(connectUpstox, 5000);
  });
}


app.get("/upstox", (req, res) => {
  const ready = Boolean(UPSTOX_API_KEY && UPSTOX_API_SECRET);
  const loginUrl = ready ? getUpstoxLoginUrl(req) : null;
  res.send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Reconnect Upstox</title><style>body{font-family:Arial,sans-serif;background:#111;color:#fff;padding:24px;line-height:1.45}.card{max-width:720px;margin:auto;background:#1d1d1d;border:1px solid #444;border-radius:18px;padding:24px}a.btn{display:inline-block;background:#d4af37;color:#111;padding:13px 18px;border-radius:12px;text-decoration:none;font-weight:700}.warn{color:#ffd36a}.ok{color:#74ff8a}code{background:#000;padding:2px 5px;border-radius:5px}</style></head><body><div class="card"><h1>R K Jewellers - Upstox Reconnect</h1><p>Status: <b>${tokenNeedsReconnect ? '<span class="warn">Reconnect required</span>' : '<span class="ok">Token present</span>'}</b></p><p>${tokenLastError || latestRates.status || ''}</p>${ready ? `<p><a class="btn" href="${loginUrl}">Reconnect Upstox Now</a></p>` : `<p class="warn">Missing environment variables. Add <code>UPSTOX_API_KEY</code>, <code>UPSTOX_API_SECRET</code>, and optionally <code>UPSTOX_REDIRECT_URI</code> in Render.</p>`}<p>Redirect URI to add in Upstox app settings:</p><p><code>${getRedirectUri(req)}</code></p><p>After reconnect, the backend will save the new access token and expiry time in MongoDB/server storage. Upstox reconnect is still available because refresh-token auto renewal is not supported in this setup.</p><p>After reconnect, open <code>/api/rates</code> again.</p></div></body></html>`);
});

async function updateRenderAccessTokenEnv(newToken) {
  const apiKey = process.env.RENDER_API_KEY || process.env.RENDER_TOKEN || "";
  const serviceId = process.env.RENDER_SERVICE_ID || "";
  const envKey = process.env.RENDER_ACCESS_TOKEN_ENV_KEY || "UPSTOX_ACCESS_TOKEN";
  if (!apiKey || !serviceId) {
    return { updated: false, reason: "Render API key/service id not configured. Token saved in MongoDB/file/server memory." };
  }
  try {
    // Best-effort Render API update. If Render changes this API, MongoDB storage still works.
    const listUrl = `https://api.render.com/v1/services/${serviceId}/env-vars`;
    const headers = { Authorization: `Bearer ${apiKey}`, Accept: "application/json", "Content-Type": "application/json" };
    const list = await axios.get(listUrl, { headers, timeout: 15000 });
    const found = Array.isArray(list.data) ? list.data.find(x => x?.envVar?.key === envKey || x?.key === envKey) : null;
    if (found?.envVar?.id || found?.id) {
      const id = found.envVar?.id || found.id;
      await axios.patch(`${listUrl}/${id}`, { value: newToken }, { headers, timeout: 15000 });
    } else {
      await axios.post(listUrl, { key: envKey, value: newToken }, { headers, timeout: 15000 });
    }
    return { updated: true };
  } catch (error) {
    return { updated: false, reason: error.response?.data || error.message };
  }
}

app.post("/api/upstox/manual-token", async (req, res) => {
  const token = String(req.body?.accessToken || "").trim();
  if (!token) return res.status(400).json({ ok: false, message: "Access token is blank." });
  await saveAccessToken({ access_token: token, source: "ATU_MANUAL_UPDATE" });
  latestRates.status = "Access token manually updated from ATU page.";
  latestRates.source = "atu-token-update";
  const renderUpdate = await updateRenderAccessTokenEnv(token);
  try { await fetchLastAvailableQuotes(); } catch {}
  try { if (currentUpstoxWs) currentUpstoxWs.close(); } catch {}
  setTimeout(connectUpstox, 1000);
  broadcast();
  res.json({ ok: true, savedToMongoDB: Boolean(tokenCollection), savedToServerFile: true, renderEnvironment: renderUpdate, accessTokenExpiresAt });
});


app.get("/api/upstox/current-token", (req, res) => {
  res.json({
    ok: Boolean(accessToken),
    accessToken: accessToken || "",
    updatedAt: accessTokenUpdatedAt || accessTokenExpiresAt || null,
    accessTokenExpiresAt,
    tokenStorage: activeTokenSource,
    mongoConnected: Boolean(tokenCollection),
    mongoWarning: getMongoFallbackMessage(),
  });
});

app.get("/api/upstox/status", (req, res) => {
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
    lastAutoRefreshAt,
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

    await saveAccessToken(response.data);
    const newTokenFromReconnect = response.data?.access_token || response.data?.accessToken;
    if (newTokenFromReconnect) {
      await updateRenderAccessTokenEnv(newTokenFromReconnect);
    }
    latestRates.status = "Upstox reconnected successfully. Fetching latest rates.";
    latestRates.source = "upstox-reconnected";
    await fetchLastAvailableQuotes();
    setTimeout(connectUpstox, 1000);

    res.send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Upstox Connected</title><style>body{font-family:Arial,sans-serif;background:#111;color:#fff;padding:24px}.card{max-width:650px;margin:auto;background:#1d1d1d;border-radius:18px;padding:24px}.ok{color:#74ff8a}a{color:#ffd36a}</style></head><body><div class="card"><h1 class="ok">Upstox connected successfully ✅</h1><p>You can close this page now.</p><p>The new access token has been saved in backend storage. You can now return to the app.</p><p><a href="/api/rates">Check live rates</a></p></div></body></html>`);
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
  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

app.post("/api/manual-rates", (req, res) => {
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
  broadcast();

  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

app.post("/api/rate-difference", (req, res) => {
  const hasGold = Object.prototype.hasOwnProperty.call(req.body, "goldDifference");
  const hasSilver = Object.prototype.hasOwnProperty.call(req.body, "silverDifference");
  const goldDiff = Number(req.body.goldDifference);
  const silverDiff = Number(req.body.silverDifference);

  const nowIso = new Date().toISOString();
  if (hasGold && Number.isFinite(goldDiff)) { goldDifference = goldDiff; goldDifferenceUpdatedAt = nowIso; }
  if (hasSilver && Number.isFinite(silverDiff)) { silverDifference = silverDiff; silverDifferenceUpdatedAt = nowIso; }

  latestRates.lastUpdated = nowIso;
  latestRates.status = "Metal rate difference updated.";
  broadcast();

  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

async function startServer() {
  await initMongoTokenStore();
  await loadSavedAccessToken();
  await prepareInstrumentKeys();
  fetchLastAvailableQuotes();
  setInterval(fetchLastAvailableQuotes, 1000);
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
