require("dotenv").config();

const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");
const axios = require("axios");
const zlib = require("zlib");
const protobuf = require("protobufjs");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 4000;
const UPSTOX_AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize";
const ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN;

// Metal rate difference from .env. You can keep blank/0 and control from frontend/API.
let goldDifference = Number(process.env.GOLD_RATE_DIFFERENCE || 0);
let silverDifference = Number(process.env.SILVER_RATE_DIFFERENCE || 0);

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

  const res = await axios.get(url, { responseType: "arraybuffer" });
  const json = zlib.gunzipSync(res.data).toString("utf8");
  const instruments = JSON.parse(json);

  const now = Date.now();
  const fiveDaysMs = 5 * 24 * 60 * 60 * 1000;
  const minimumAllowedExpiry = now + fiveDaysMs;

  function find(symbol) {
    return instruments
      .filter((x) =>
        x.segment === "MCX_FO" &&
        x.instrument_type === "FUT" &&
        x.asset_symbol === symbol &&
        Number(x.expiry) > minimumAllowedExpiry
      )
      .sort((a, b) => Number(a.expiry) - Number(b.expiry))[0];
  }

  const gold = find("GOLD");
  const silver = find("SILVER");

  if (!gold || !silver) {
    throw new Error("Could not find active MCX GOLD/SILVER futures after applying 5-day expiry skip rule.");
  }

  return { gold, silver };
}

async function prepareInstrumentKeys() {
  try {
    const autoKeys = await getAutoKeys();

    GOLD_KEY = process.env.MANUAL_GOLD_KEY || autoKeys.gold.instrument_key;
    SILVER_KEY = process.env.MANUAL_SILVER_KEY || autoKeys.silver.instrument_key;

    console.log("Using GOLD:", autoKeys.gold.trading_symbol, GOLD_KEY);
    console.log("Using SILVER:", autoKeys.silver.trading_symbol, SILVER_KEY);

    latestRates.status = "Instrument keys loaded successfully. Contracts expiring within next 5 days are skipped.";
  } catch (error) {
    GOLD_KEY = process.env.MANUAL_GOLD_KEY || process.env.GOLD_INSTRUMENT_KEY || null;
    SILVER_KEY = process.env.MANUAL_SILVER_KEY || process.env.SILVER_INSTRUMENT_KEY || null;

    console.log("Auto key download failed:", error.message);

    if (GOLD_KEY && SILVER_KEY) {
      console.log("Using manual keys.");
      latestRates.status = "Auto key download failed. Using manual instrument keys.";
    } else {
      latestRates.status = "Auto key download failed and manual keys are missing.";
    }
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
    goldInstrumentKey: GOLD_KEY,
    silverInstrumentKey: SILVER_KEY,
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

async function getAuthorizedWebSocketUrl() {
  const response = await axios.get(UPSTOX_AUTHORIZE_URL, {
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
      Accept: "application/json",
    },
  });

  return response.data.data.authorized_redirect_uri;
}

async function connectUpstox() {
  if (!ACCESS_TOKEN || !GOLD_KEY || !SILVER_KEY) {
    latestRates.status = "Missing access token or instrument keys. Manual mode available.";
    latestRates.source = "manual";
    console.log(latestRates.status);
    return;
  }

  let authorizedUrl;
  try {
    authorizedUrl = await getAuthorizedWebSocketUrl();
  } catch (error) {
    latestRates.status = `WebSocket authorize error: ${error.response?.data?.errors?.[0]?.message || error.message}`;
    latestRates.source = "error";
    console.error(latestRates.status);
    broadcast();
    setTimeout(connectUpstox, 10000);
    return;
  }

  const ws = new WebSocket(authorizedUrl, {
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
    },
  });

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
        broadcast();
      } else if (data?.type === "market_info") {
        latestRates.status = "Market info received. Waiting for live prices.";
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

app.get("/api/rates", (req, res) => {
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
  broadcast();

  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

app.post("/api/rate-difference", (req, res) => {
  const hasGold = Object.prototype.hasOwnProperty.call(req.body, "goldDifference");
  const hasSilver = Object.prototype.hasOwnProperty.call(req.body, "silverDifference");
  const goldDiff = Number(req.body.goldDifference);
  const silverDiff = Number(req.body.silverDifference);

  if (hasGold && Number.isFinite(goldDiff)) goldDifference = goldDiff;
  if (hasSilver && Number.isFinite(silverDiff)) silverDifference = silverDiff;

  latestRates.lastUpdated = new Date().toISOString();
  latestRates.status = "Metal rate difference updated.";
  broadcast();

  res.json(calculateRates(latestRates.goldMcx, latestRates.silverMcx));
});

async function startServer() {
  await prepareInstrumentKeys();

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
