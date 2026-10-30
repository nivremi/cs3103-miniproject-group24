/*
 * LiveRecall browser client.
 * URL params: ?server=ws://host:8765&session=L1
 */
(function () {
  "use strict";

  const params = new URLSearchParams(location.search);
  const SESSION = params.get("session") || "L1";
  const SERVER = params.get("server") ||
    `ws://${location.hostname || "127.0.0.1"}:8765`;
  const HELLO_HISTORY_SECONDS = 60;   // ask for the full recall window on join
  const DEFAULT_RECALL_SECONDS = 30;
  const MAX_LIVE_ROWS = 60;           // DOM rows kept in the live view
  const BACKOFF_MIN_MS = 500;
  const BACKOFF_MAX_MS = 5000;

  const store = new CaptionStore();
  const el = {
    conn: document.getElementById("conn"),
    connText: document.getElementById("conn-text"),
    live: document.getElementById("live"),
    empty: document.getElementById("empty"),
    jump: document.getElementById("jump"),
    miss: document.getElementById("miss"),
    recall: document.getElementById("recall"),
    recallList: document.getElementById("recall-list"),
    recallMeta: document.getElementById("recall-meta"),
    recallClose: document.getElementById("recall-close"),
    recallRefresh: document.getElementById("recall-refresh"),
    tabs: Array.from(document.querySelectorAll(".seg button")),
    toast: document.getElementById("toast"),
  };
  document.getElementById("session-label").textContent = `Session ${SESSION}`;

  let ws = null;
  let attempt = 0;
  let everConnected = false;
  let reconnectTimer = null;
  let countdownTimer = null;
  let recallSeconds = DEFAULT_RECALL_SECONDS;
  let recallSnapshotSeg = 0;
  let followLive = true;
  let reconnectedFlag = false;   // true until the HISTORY after a reconnect
  const rows = new Map();   // segment -> row element

  // ------------------------------------------------------------------ ids
  function numericClientId() {
    const key = "liverecall-client-id";
    try {
      let value = sessionStorage.getItem(key);
      if (!value) {
        value = String(Math.floor(100000 + Math.random() * 900000));
        sessionStorage.setItem(key, value);
      }
      return Number(value);
    } catch (e) {
      return Math.floor(100000 + Math.random() * 900000);
    }
  }
  const CLIENT_ID = numericClientId();

  // ------------------------------------------------------------- status UI
  function setConn(state, text) {
    el.conn.dataset.state = state;
    el.connText.textContent = text;
  }

  let toastTimer = null;
  function toast(text, kind) {
    el.toast.textContent = text;
    el.toast.className = "toast" + (kind ? " " + kind : "");
    el.toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { el.toast.hidden = true; }, 4000);
  }

  // ------------------------------------------------------------ live view
  function nearBottom() {
    const l = el.live;
    return l.scrollHeight - l.scrollTop - l.clientHeight < 60;
  }

  function scrollToLive() {
    el.live.scrollTop = el.live.scrollHeight;
    el.jump.hidden = true;
    followLive = true;
  }

  el.live.addEventListener("scroll", () => {
    followLive = nearBottom();
    if (followLive) el.jump.hidden = true;
  });
  el.jump.addEventListener("click", scrollToLive);

  function renderRow(entry, recovered) {
    let row = rows.get(entry.segment);
    const wasProvisional = row && row.classList.contains("prov");
    if (!row) {
      row = document.createElement("div");
      row.className = "cap";
      row.dataset.segment = String(entry.segment);
      row.innerHTML = '<span class="text"></span><span class="meta"></span>';
      insertInOrder(row, entry.segment);
      rows.set(entry.segment, row);
      el.empty.hidden = true;
    }
    row.querySelector(".text").textContent = entry.text;
    row.querySelector(".meta").textContent = `#${entry.segment} · r${entry.revision}`;
    row.classList.toggle("prov", !entry.final);
    row.classList.toggle("final", entry.final);
    if (recovered) row.classList.add("recovered");
    if (entry.final && wasProvisional) {
      row.classList.add("just-final");
      setTimeout(() => row.classList.remove("just-final"), 700);
    }
    trimRows();
  }

  function insertInOrder(row, segment) {
    // Usually appended at the end; HISTORY items may need to go earlier.
    let before = null;
    for (const [seg, r] of rows) {
      if (seg > segment && (!before || seg < Number(before.dataset.segment))) before = r;
    }
    el.live.insertBefore(row, before);
  }

  function addMarker(text) {
    const m = document.createElement("div");
    m.className = "marker";
    m.textContent = text;
    const segs = Array.from(rows.keys());
    const after = segs.length ? Math.max(...segs) : 0;
    m.dataset.after = String(after);
    el.live.appendChild(m);
  }

  function trimRows() {
    if (rows.size <= MAX_LIVE_ROWS) return;
    const sorted = Array.from(rows.keys()).sort((a, b) => a - b);
    for (const seg of sorted.slice(0, rows.size - MAX_LIVE_ROWS)) {
      rows.get(seg).remove();
      rows.delete(seg);
    }
    const oldest = Math.min(...rows.keys());
    for (const m of el.live.querySelectorAll(".marker")) {
      if (Number(m.dataset.after) < oldest) m.remove();
    }
  }

  function afterRender() {
    if (followLive) scrollToLive();
    else el.jump.hidden = false;
    if (!el.recall.hidden) updateRefreshHint();
  }

  // ---------------------------------------------------------- recall view
  function fmtAgo(entry) {
    const s = Math.max(0, Math.round(store.latestTs - entry.source_ts));
    return s === 0 ? "now" : `−${s} s`;
  }

  function renderRecall() {
    const items = store.recent(recallSeconds);
    el.recallList.innerHTML = "";
    let count = 0;
    for (const item of items) {
      const li = document.createElement("li");
      if (item.gap) {
        const span = document.createElement("span");
        span.className = "gap";
        span.textContent = `⋯ ${item.gap} caption${item.gap > 1 ? "s" : ""} unavailable (missed while offline)`;
        li.appendChild(span);
        li.style.display = "block";
      } else {
        count += 1;
        const ago = document.createElement("span");
        ago.className = "ago";
        ago.textContent = fmtAgo(item);
        const text = document.createElement("span");
        text.textContent = item.text;
        li.append(ago, text);
      }
      el.recallList.appendChild(li);
    }
    if (count === 0) {
      const li = document.createElement("li");
      li.style.display = "block";
      li.textContent = "Nothing finalized in this window yet.";
      el.recallList.appendChild(li);
    }
    el.recallMeta.textContent =
      `Last ${recallSeconds} s of the lecture · ${count} caption${count === 1 ? "" : "s"} · from this device`;
    el.tabs.forEach((t) => t.setAttribute("aria-selected", String(Number(t.dataset.seconds) === recallSeconds)));
    recallSnapshotSeg = store.lastFinalSegment;
    el.recallRefresh.hidden = true;
    el.recallList.scrollTop = el.recallList.scrollHeight;
  }

  function updateRefreshHint() {
    const n = store.lastFinalSegment - recallSnapshotSeg;
    el.recallRefresh.hidden = n <= 0;
    el.recallRefresh.textContent = `Show ${n} newer caption${n === 1 ? "" : "s"}`;
  }

  function openRecall(seconds) {
    recallSeconds = seconds || DEFAULT_RECALL_SECONDS;
    el.recall.hidden = false;
    renderRecall();
  }

  function closeRecall() {
    el.recall.hidden = true;
    el.miss.focus();
  }

  el.miss.addEventListener("click", () => (el.recall.hidden ? openRecall(DEFAULT_RECALL_SECONDS) : closeRecall()));
  el.recallClose.addEventListener("click", closeRecall);
  el.recallRefresh.addEventListener("click", renderRecall);
  el.tabs.forEach((t) => t.addEventListener("click", () => {
    recallSeconds = Number(t.dataset.seconds);
    renderRecall();
  }));
  document.addEventListener("keydown", (e) => {
    if (e.target.closest && e.target.closest("input, textarea")) return;
    if (e.key === "m" || e.key === "M") {
      if (el.recall.hidden) openRecall(DEFAULT_RECALL_SECONDS); else closeRecall();
    } else if (e.key === "Escape" && !el.recall.hidden) {
      closeRecall();
    }
  });

  // ----------------------------------------------------------- networking
  function handleMessage(event) {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch (e) {
      return;
    }
    if (msg.type === "CAPTION_UPDATE") {
      const res = store.apply(msg, "live");
      if (res.status === "accepted") {
        renderRow(res.entry, false);
        afterRender();
      }
    } else if (msg.type === "HISTORY") {
      const before = new Set(store.segments.keys());
      const recovered = store.applyHistory(msg.items);
      for (const item of msg.items || []) {
        const entry = store.segments.get(item.segment);
        if (entry && !before.has(item.segment)) renderRow(entry, true);
      }
      afterRender();
      if (reconnectedFlag) {
        toast(recovered ? `Reconnected — recovered ${recovered} missed caption${recovered === 1 ? "" : "s"}`
                        : "Reconnected — nothing missed");
        if (recovered) addMarker(`recovered ${recovered} after reconnect`);
      } else if (recovered) {
        toast(`Joined — loaded ${recovered} recent caption${recovered === 1 ? "" : "s"}`);
      }
      if (msg.gap) toast("Some captions from while you were offline are no longer available", "warn");
      reconnectedFlag = false;
    }
  }

  function connect() {
    clearTimeout(reconnectTimer);
    clearInterval(countdownTimer);
    setConn(everConnected ? "reconnecting" : "connecting",
      everConnected ? "Reconnecting…" : "Connecting…");
    let socket;
    try {
      socket = new WebSocket(SERVER);
    } catch (e) {
      scheduleReconnect();
      return;
    }
    ws = socket;

    socket.addEventListener("open", () => {
      if (socket !== ws) return;
      reconnectedFlag = everConnected;
      const resume = store.resumeState();
      socket.send(JSON.stringify({
        type: "CLIENT_HELLO",
        client_id: CLIENT_ID,
        session: SESSION,
        last_segment: resume.last_segment,
        last_revision: resume.last_revision,
        history_seconds: HELLO_HISTORY_SECONDS,
      }));
      everConnected = true;
      attempt = 0;
      setConn("connected", "Live");
    });

    socket.addEventListener("message", (event) => {
      if (socket === ws) handleMessage(event);
    });

    socket.addEventListener("close", () => {
      if (socket !== ws) return;
      ws = null;
      scheduleReconnect();
    });
    // "error" is always followed by "close"; nothing extra to do.
  }

  function scheduleReconnect() {
    const base = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** attempt);
    const delay = Math.round(base * (0.75 + Math.random() * 0.5)); // jitter: avoid reconnect storms
    attempt += 1;
    const due = Date.now() + delay;
    const label = () => {
      const s = Math.max(0, Math.ceil((due - Date.now()) / 1000));
      setConn(everConnected ? "reconnecting" : "offline",
        `${everConnected ? "Disconnected" : "Server unreachable"} — retrying in ${s}s (attempt ${attempt})`);
    };
    label();
    clearInterval(countdownTimer);
    countdownTimer = setInterval(label, 250);
    reconnectTimer = setTimeout(connect, delay);
  }

  // Reconnect immediately when the network or tab comes back.
  window.addEventListener("online", () => { if (!ws) { attempt = 0; connect(); } });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && !ws) { attempt = 0; connect(); }
  });

  // Exposed for debugging and automated UI tests.
  window.liveRecall = {
    store,
    disconnect() { if (ws) ws.close(); },
  };

  connect();
})();
