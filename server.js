const http = require("http");
const net = require("net");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "10000", 10);
const TOKEN = process.env.STREAM_TOKEN || "";
const UPSTREAM_HOST_1 = process.env.UPSTREAM_HOST_1 || "127.0.0.1";
const UPSTREAM_HOST_2 = process.env.UPSTREAM_HOST_2 || UPSTREAM_HOST_1;
const UPSTREAM_PORT = parseInt(process.env.UPSTREAM_PORT || "9000", 10);

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok\n");
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false });

server.on("upgrade", (req, socket, head) => {
  if (TOKEN && req.headers["x-stream-token"] !== TOKEN) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    return socket.destroy();
  }
  const host = req.headers["x-stream-pool"] === "2" ? UPSTREAM_HOST_2 : UPSTREAM_HOST_1;
  wss.handleUpgrade(req, socket, head, (ws) => {
    const upstream = net.connect(UPSTREAM_PORT, host);
    upstream.setNoDelay(true);
    upstream.setKeepAlive(true, 15000);

    upstream.on("data", (chunk) => {
      if (ws.readyState === ws.OPEN) {
        ws.send(chunk, { binary: true });
      }
    });
    ws.on("message", (data) => {
      if (!upstream.destroyed) {
        upstream.write(Buffer.from(data));
      }
    });

    const cleanup = () => {
      try { upstream.destroy(); } catch (_) {}
      try { ws.close(); } catch (_) {}
    };
    upstream.on("error", cleanup);
    upstream.on("close", cleanup);
    ws.on("error", cleanup);
    ws.on("close", cleanup);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ipc-stream-gw listening on :${PORT}`);
});
