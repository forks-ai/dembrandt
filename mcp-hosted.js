#!/usr/bin/env node

/**
 * Dembrandt Hosted MCP Server
 *
 * Same tools as mcp-server.js, but served over HTTP/SSE
 * so users don't need to install anything locally.
 *
 * Usage:
 *   node mcp-hosted.js              # starts on port 3001
 *   PORT=8080 node mcp-hosted.js    # custom port
 */

import { createServer } from "node:http";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { z } from "zod";
import { chromium } from "playwright-core";
import { readFileSync } from "node:fs";
import { extractBranding } from "./lib/extractors.js";

const { version } = JSON.parse(readFileSync(new URL("./package.json", import.meta.url), "utf8"));

// ── Shared helpers (same as mcp-server.js) ────────────────────────────

const nullSpinner = {
  text: "",
  start(msg) { this.text = msg; return this; },
  stop() { return this; },
  succeed(msg) { return this; },
  fail(msg) { return this; },
  warn(msg) { return this; },
  info(msg) { return this; },
};

async function runExtraction(url, options = {}) {
  if (!/^https?:\/\//i.test(url)) url = "https://" + url;
  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ["--disable-blink-features=AutomationControlled"],
    });
  } catch (err) {
    return {
      ok: false,
      error: `Browser launch failed: ${err.message}`,
    };
  }

  const _log = console.log;
  const _warn = console.warn;
  const _error = console.error;
  console.log = () => {};
  console.warn = () => {};
  console.error = () => {};

  try {
    const data = await extractBranding(url, nullSpinner, browser, {
      navigationTimeout: 90000,
      slow: options.slow || false,
      darkMode: options.darkMode || false,
      mobile: options.mobile || false,
    });
    return { ok: true, data };
  } catch (err) {
    const msg = err.message || String(err);
    if (msg.includes("timeout") || msg.includes("Timeout")) {
      return { ok: false, error: `Extraction timed out for ${url}. Try with slow: true.` };
    }
    if (msg.includes("net::ERR_NAME_NOT_RESOLVED")) {
      return { ok: false, error: `Could not resolve ${url}. Check the URL.` };
    }
    if (msg.includes("net::ERR_CONNECTION_REFUSED")) {
      return { ok: false, error: `Connection refused by ${url}.` };
    }
    return { ok: false, error: `Extraction failed for ${url}: ${msg}` };
  } finally {
    console.log = _log;
    console.warn = _warn;
    console.error = _error;
    await browser.close().catch(() => {});
  }
}

function jsonResult(data) {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function errorResult(message) {
  return { content: [{ type: "text", text: message }], isError: true };
}

// ── Job Queue ─────────────────────────────────────────────────────────

class JobQueue {
  #jobs = new Map();
  #queue = [];
  #running = new Set();
  #maxConcurrent = 2;

  enqueue(url, opts, pick) {
    const id = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    this.#jobs.set(id, {
      status: "queued",
      url,
      opts,
      pick,
      createdAt: Date.now(),
      startedAt: undefined,
      completedAt: undefined,
      result: undefined,
      error: undefined,
    });
    this.#queue.push(id);
    void this.#drain();
    return id;
  }

  get(id) {
    return this.#jobs.get(id) ?? null;
  }

  cancel(id) {
    const job = this.#jobs.get(id);
    if (!job || job.status !== "queued") return false;
    job.status = "cancelled";
    job.completedAt = Date.now();
    const idx = this.#queue.indexOf(id);
    if (idx !== -1) this.#queue.splice(idx, 1);
    return true;
  }

  async #drain() {
    while (this.#queue.length > 0 && this.#running.size < this.#maxConcurrent) {
      const id = this.#queue.shift();
      const job = this.#jobs.get(id);
      if (!job || job.status === "cancelled") continue;

      job.status = "running";
      job.startedAt = Date.now();
      this.#running.add(id);

      runExtraction(job.url, job.opts)
        .then((result) => {
          if (job.status === "cancelled") return;
          if (result.ok) {
            job.status = "completed";
            job.result = job.pick(result.data);
          } else {
            job.status = "failed";
            job.error = result.error;
          }
        })
        .catch((err) => {
          if (job.status !== "cancelled") {
            job.status = "failed";
            job.error = err.message || String(err);
          }
        })
        .finally(() => {
          job.completedAt = Date.now();
          this.#running.delete(id);
          void this.#drain();
        });
    }
  }

  cleanup() {
    const cutoff = Date.now() - 3_600_000;
    for (const [id, job] of this.#jobs) {
      if (
        ["completed", "failed", "cancelled"].includes(job.status) &&
        job.completedAt !== undefined &&
        job.completedAt < cutoff
      ) {
        this.#jobs.delete(id);
      }
    }
  }
}

const jobQueue = new JobQueue();
setInterval(() => jobQueue.cleanup(), 600_000);

function toolHandler(pick, extraOptions = {}) {
  return async (params) => {
    const { url, slow, darkMode, sync } = params;
    const opts = { slow, darkMode, ...extraOptions };

    if (sync) {
      const result = await runExtraction(url, opts);
      if (!result.ok) return errorResult(result.error);
      return jsonResult(pick(result.data));
    }

    const jobId = jobQueue.enqueue(url, opts, pick);
    return jsonResult({ job_id: jobId, status: "queued" });
  };
}

// ── Create MCP server with tools ──────────────────────────────────────

function createMcpServer() {
  const server = new McpServer({
    name: "dembrandt-pro",
    version,
  });

  const url = z.string().describe("Website URL (e.g. example.com)");
  const slow = z.boolean().optional().default(false).describe("3x timeouts for heavy SPAs");
  const sync = z.boolean().optional().default(false).describe("Wait for result directly instead of returning a job_id (blocks 15-40s)");

  server.tool("get_design_tokens", "Extract the full design system from a live website. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => d));
  server.tool("get_color_palette", "Extract brand colors from a live website. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync, darkMode: z.boolean().optional().default(false).describe("Also extract dark mode palette") }, toolHandler((d) => ({ url: d.url, colors: d.colors })));
  server.tool("get_typography", "Extract typography from a live website. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => ({ url: d.url, typography: d.typography })));
  server.tool("get_component_styles", "Extract UI component styles from a live website. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => ({ url: d.url, components: d.components })));
  server.tool("get_surfaces", "Extract surface tokens: border radii, borders, shadows. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => ({ url: d.url, borderRadius: d.borderRadius, borders: d.borders, shadows: d.shadows })));
  server.tool("get_spacing", "Extract the spacing system from a live website. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => ({ url: d.url, spacing: d.spacing })));
  server.tool("get_brand_identity", "Extract brand identity: logo, favicons, frameworks. Returns a job_id by default — use get_job_status to poll for the result.", { url, slow, sync }, toolHandler((d) => ({ url: d.url, siteName: d.siteName, logo: d.logo, favicons: d.favicons, frameworks: d.frameworks, iconSystem: d.iconSystem, breakpoints: d.breakpoints })));

  server.tool(
    "get_job_status",
    "Poll for the result of an async extraction job. Returns status (queued/running/completed/failed/cancelled) and the full result once completed.",
    { job_id: z.string().describe("The job_id returned by an extraction tool") },
    ({ job_id }) => {
      const job = jobQueue.get(job_id);
      if (!job) return errorResult(`No job found with id: ${job_id}`);
      if (job.status === "completed") return jsonResult({ job_id, status: "completed", result: job.result });
      if (job.status === "failed") return errorResult(`Job failed: ${job.error}`);
      return jsonResult({ job_id, status: job.status });
    },
  );

  server.tool(
    "cancel_job",
    "Cancel a queued extraction job. Has no effect on jobs that are already running.",
    { job_id: z.string().describe("The job_id to cancel") },
    ({ job_id }) => {
      const cancelled = jobQueue.cancel(job_id);
      return jsonResult({ job_id, cancelled });
    },
  );

  return server;
}

// ── HTTP server with SSE transport ────────────────────────────────────

const PORT = process.env.PORT || 3001;
const sessions = new Map();

const httpServer = createServer(async (req, res) => {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  // Health check
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", version }));
    return;
  }

  // SSE endpoint - client connects here
  if (req.url === "/sse" && req.method === "GET") {
    // TODO: API key auth here later
    // const apiKey = req.headers.authorization?.replace("Bearer ", "");

    const server = createMcpServer();
    const transport = new SSEServerTransport("/messages", res);
    sessions.set(transport.sessionId, { server, transport });

    transport.onclose = () => sessions.delete(transport.sessionId);

    await server.connect(transport);
    return;
  }

  // Message endpoint - client sends MCP messages here
  if (req.url?.startsWith("/messages") && req.method === "POST") {
    const sessionId = new URL(req.url, `http://localhost:${PORT}`).searchParams.get("sessionId");
    const session = sessions.get(sessionId);
    if (!session) {
      res.writeHead(404);
      res.end("Session not found");
      return;
    }

    // Read body
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks).toString();

    // Pass to transport
    const fakeReq = { ...req, body };
    await session.transport.handlePostMessage(fakeReq, res);
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

httpServer.listen(PORT, () => {
  console.log(`Dembrandt Pro MCP server running on http://localhost:${PORT}`);
  console.log(`Connect your MCP client to http://localhost:${PORT}/sse`);
});
