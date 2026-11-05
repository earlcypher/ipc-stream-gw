const http = require("http");
const https = require("https");
const net = require("net");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "10000", 10);
const TOKEN = process.env.STREAM_TOKEN || "";
const CONNECTOR_BIN_URL = process.env.CONNECTOR_BIN_URL || "";
const CONNECTOR_UUID = process.env.CONNECTOR_UUID || "";
const CONNECTOR_CRED_JSON = process.env.CONNECTOR_CRED_JSON || "";
// Format: "hostname:localListenPort:remoteTargetPort,..."
const ROUTE_RULES = (process.env.ROUTE_RULES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map((entry) => {
    const [hostname, listenPort, targetPort] = entry.split(":");
    return {
      hostname,
      listenPort: parseInt(listenPort, 10),
      targetPort: parseInt(targetPort, 10),
    };
  });

let activeAgent = null;
let activeBootTs = 0;
let activeHost = "none";
let connectorReady = false;
const pending = new Map(); // id -> { sock, timer }

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    // Public liveness only. Internal agent details are not exposed.
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true }) + "\n");
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 0 });

server.on("upgrade", (req, socket, head) => {
  if (TOKEN && req.headers["x-stream-token"] !== TOKEN) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    return socket.destroy();
  }
  const url = new URL(req.url, "http://localhost");

  if (url.pathname === "/_ctrl") {
    const bootTs = parseFloat(req.headers["x-boot-ts"] || "0") || 0;
    const host = String(req.headers["x-agent-host"] || "unknown");
    wss.handleUpgrade(req, socket, head, (ws) => {
      if (activeAgent && activeAgent.readyState === activeAgent.OPEN && bootTs > 0 && bootTs < activeBootTs) {
        ws.close(4009, "superseded by newer boot_ts");
        return;
      }
      if (activeAgent && activeAgent !== ws) {
        try {
          activeAgent.close(4009, "replaced");
        } catch (_) {}
      }
      activeAgent = ws;
      if (bootTs >= activeBootTs) activeBootTs = bootTs;
      activeHost = host;
      console.log(`[ctrl] agent connected: host=${host} bootTs=${bootTs}`);

      ws.on("message", (msg) => {
        try {
          const data = JSON.parse(msg.toString());
          if (data.op === "fail" && data.id && pending.has(data.id)) {
            const p = pending.get(data.id);
            pending.delete(data.id);
            clearTimeout(p.timer);
            try { p.sock.destroy(); } catch (_) {}
          }
        } catch (_) {}
      });

      ws.on("close", () => {
        if (activeAgent === ws) {
          console.log(`[ctrl] agent disconnected: host=${host}`);
          activeAgent = null;
        }
      });
      ws.on("error", () => {});
    });
    return;
  }

  if (url.pathname === "/_data") {
    const id = url.searchParams.get("id") || "";
    const item = pending.get(id);
    if (!item) {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      return socket.destroy();
    }
    pending.delete(id);
    clearTimeout(item.timer);
    const tcpSock = item.sock;

    wss.handleUpgrade(req, socket, head, (ws) => {
      tcpSock.setNoDelay(true);
      tcpSock.on("data", (chunk) => {
        if (ws.readyState === ws.OPEN) {
          ws.send(chunk, { binary: true });
        }
      });
      ws.on("message", (data) => {
        if (!tcpSock.destroyed) {
          tcpSock.write(Buffer.from(data));
        }
      });
      const cleanup = () => {
        try { tcpSock.destroy(); } catch (_) {}
        try { ws.close(); } catch (_) {}
      };
      tcpSock.on("error", cleanup);
      tcpSock.on("close", cleanup);
      ws.on("error", cleanup);
      ws.on("close", cleanup);
      tcpSock.resume();
    });
    return;
  }

  socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
  socket.destroy();
});

// Start local TCP listeners for each configured route rule
for (const rule of ROUTE_RULES) {
  const tcpSrv = net.createServer({ pauseOnConnect: true }, (sock) => {
    if (!activeAgent || activeAgent.readyState !== activeAgent.OPEN) {
      sock.destroy();
      return;
    }
    const id = crypto.randomBytes(12).toString("hex");
    const timer = setTimeout(() => {
      pending.delete(id);
      try { sock.destroy(); } catch (_) {}
    }, 15000);
    pending.set(id, { sock, timer });
    sock.on("error", () => {
      pending.delete(id);
      clearTimeout(timer);
    });
    sock.on("close", () => {
      pending.delete(id);
      clearTimeout(timer);
    });
    try {
      activeAgent.send(JSON.stringify({ op: "open", id, port: rule.targetPort }));
    } catch (_) {
      pending.delete(id);
      clearTimeout(timer);
      sock.destroy();
    }
  });
  tcpSrv.listen(rule.listenPort, "127.0.0.1", () => {
    console.log(`[route] 127.0.0.1:${rule.listenPort} -> agent 127.0.0.1:${rule.targetPort} (${rule.hostname})`);
  });
}

function downloadFile(url, dest, redirects = 5) {
  return new Promise((resolve, reject) => {
    if (redirects <= 0) return reject(new Error("too many redirects"));
    https.get(url, { headers: { "User-Agent": "curl/8.5.0" } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(downloadFile(res.headers.location, dest, redirects - 1));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const file = fs.createWriteStream(dest, { mode: 0o755 });
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
      file.on("error", reject);
    }).on("error", reject);
  });
}

async function startConnector() {
  if (!CONNECTOR_BIN_URL || !CONNECTOR_UUID || !CONNECTOR_CRED_JSON || ROUTE_RULES.length === 0) {
    console.log("[connector] skipped (env vars not set)");
    return;
  }
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "ipc-conn-"));
  const binPath = path.join(workDir, "connector-bin");
  const credPath = path.join(workDir, `${CONNECTOR_UUID}.json`);
  const cfgPath = path.join(workDir, "config.yml");

  fs.writeFileSync(credPath, CONNECTOR_CRED_JSON, { mode: 0o600 });
  const lines = [
    `tunnel: ${CONNECTOR_UUID}`,
    `credentials-file: ${credPath}`,
    "protocol: http2",
    "no-autoupdate: true",
    // Drop idle origin keep-alive sockets almost immediately. The local relay
    // can hand back a socket the origin already closed (-> 502) if reused.
    "originRequest:",
    "  keepAliveTimeout: 1ms",
    "ingress:",
  ];
  for (const r of ROUTE_RULES) {
    lines.push(`  - hostname: ${r.hostname}`);
    lines.push(`    service: http://127.0.0.1:${r.listenPort}`);
    lines.push("    originRequest:");
    lines.push("      keepAliveTimeout: 1ms");
  }
  lines.push("  - service: http_status:404");
  fs.writeFileSync(cfgPath, lines.join("\n") + "\n", { mode: 0o600 });

  await downloadFile(CONNECTOR_BIN_URL, binPath);
  fs.chmodSync(binPath, 0o755);

  const launch = () => {
    const proc = spawn(binPath, ["tunnel", "--config", cfgPath, "--protocol", "http2", "--no-autoupdate", "run", CONNECTOR_UUID], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const onOut = (buf) => {
      const s = buf.toString();
      if (s.includes("Registered tunnel connection")) connectorReady = true;
    };
    proc.stdout.on("data", onOut);
    proc.stderr.on("data", onOut);
    proc.on("exit", (code) => {
      connectorReady = false;
      console.log(`[connector] exited (${code}) -> restarting in 3s`);
      setTimeout(launch, 3000);
    });
  };
  launch();
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ipc-stream-gw listening on :${PORT}`);
  startConnector().catch((err) => console.error("[connector] init error:", err.message));
});
