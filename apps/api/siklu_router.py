from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import HTMLResponse

from packages.jake.connectors.siklu_alignment import get_siklu_alignment_service

router = APIRouter(tags=["siklu"])


@router.get("/v1/siklu/{ip}/alignment")
def siklu_alignment(ip: str, poll_ms: int = Query(default=750, ge=250, le=1000)) -> dict:
    try:
        return get_siklu_alignment_service().snapshot(ip, poll_ms=poll_ms)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/v1/siklu/{ip}/reset-peak")
def siklu_reset_peak(ip: str, poll_ms: int = Query(default=750, ge=250, le=1000)) -> dict:
    try:
        return get_siklu_alignment_service().reset_peak(ip, poll_ms=poll_ms)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/v1/siklu/{ip}/reset-session")
def siklu_reset_session(ip: str, poll_ms: int = Query(default=750, ge=250, le=1000)) -> dict:
    try:
        return get_siklu_alignment_service().reset_web_session(ip, poll_ms=poll_ms)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v1/siklu-link/{ip_a}/{ip_b}/notes")
def siklu_link_notes(ip_a: str, ip_b: str) -> dict:
    try:
        return get_siklu_alignment_service().get_link_notes(ip_a, ip_b)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/v1/siklu-link/{ip_a}/{ip_b}/notes")
def siklu_save_link_notes(ip_a: str, ip_b: str, payload: dict = Body(default={})) -> dict:
    try:
        return get_siklu_alignment_service().save_link_notes(ip_a, ip_b, str(payload.get("text") or ""))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/siklu/{ip}", response_class=HTMLResponse)
def siklu_alignment_page(ip: str, poll_ms: int = Query(default=750, ge=250, le=1000)) -> str:
    safe_ip = str(ip).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>Siklu Align {safe_ip}</title>
  <style>
    :root {{
      --bg: #06111a;
      --panel: rgba(10, 25, 39, 0.82);
      --line: rgba(103, 203, 255, 0.34);
      --text: #ebf6ff;
      --muted: #93a8ba;
      --hot: #67ff8e;
      --warn: #ffb347;
      --cold: #ff6b6b;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top, rgba(79, 176, 255, 0.18), transparent 38%),
        linear-gradient(180deg, #071019 0%, #03070c 100%);
      color: var(--text);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      display: grid;
      place-items: center;
    }}
    .app {{
      width: min(96vw, 560px);
      padding: 18px;
      display: grid;
      gap: 14px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 18px 50px rgba(0,0,0,0.35);
      backdrop-filter: blur(16px);
    }}
    .title {{
      font-size: 28px;
      font-weight: 800;
      letter-spacing: 0.04em;
    }}
    .sub {{
      color: var(--muted);
      margin-top: 6px;
      font-size: 13px;
    }}
    .bubble-wrap {{
      aspect-ratio: 1 / 1;
      position: relative;
      overflow: hidden;
    }}
    .bubble-ring {{
      position: absolute;
      inset: 0;
      border-radius: 50%;
      border: 1px solid rgba(103, 203, 255, 0.42);
      background:
        radial-gradient(circle at center, rgba(103, 203, 255, 0.08), transparent 45%),
        repeating-radial-gradient(circle at center, rgba(103,203,255,0.14) 0 2px, transparent 2px 46px);
    }}
    .bubble {{
      width: 58px;
      height: 58px;
      border-radius: 50%;
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      background: radial-gradient(circle at 35% 35%, #fff6, #67cbff 60%, #0ea5e9 100%);
      box-shadow: 0 0 20px rgba(103, 203, 255, 0.6);
    }}
    .peak {{
      width: 18px;
      height: 18px;
      border-radius: 50%;
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      border: 2px solid var(--hot);
      box-shadow: 0 0 16px rgba(103,255,142,0.45);
      opacity: 0.85;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }}
    .metric {{
      background: rgba(255,255,255,0.02);
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 14px;
      padding: 12px;
    }}
    .metric .k {{ color: var(--muted); font-size: 12px; }}
    .metric .v {{ margin-top: 5px; font-size: 28px; font-weight: 700; }}
    .hint {{
      font-size: 17px;
      font-weight: 700;
      color: var(--warn);
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.08);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    .badge.ok {{ color: var(--hot); border-color: rgba(103,255,142,0.28); }}
    .badge.warn {{ color: var(--warn); border-color: rgba(255,179,71,0.28); }}
    .badge.bad {{ color: var(--cold); border-color: rgba(255,107,107,0.28); }}
    .banner {{
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.08);
      font-size: 13px;
    }}
    .banner.bad {{ color: var(--cold); background: rgba(255,107,107,0.08); }}
    .banner.warn {{ color: var(--warn); background: rgba(255,179,71,0.08); }}
    button {{
      border: 1px solid var(--line);
      background: rgba(103, 203, 255, 0.08);
      color: var(--text);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
    }}
  </style>
</head>
<body>
  <div class="app">
    <div class="panel">
      <div class="title">Siklu Align</div>
      <div class="sub">{safe_ip} · local alignment view · poll {poll_ms}ms</div>
      <div style="display:flex; gap:10px; margin-top:10px; flex-wrap:wrap;">
        <div class="badge warn" id="sourceBadge">source unknown</div>
        <div class="badge warn" id="freshBadge">waiting</div>
      </div>
      <div class="banner warn" id="banner">Waiting for first sample.</div>
    </div>
    <div class="panel bubble-wrap">
      <div class="bubble-ring"></div>
      <div class="peak" id="peak"></div>
      <div class="bubble" id="bubble"></div>
    </div>
    <div class="grid">
      <div class="metric"><div class="k">RSSI</div><div class="v" id="rssi">--</div></div>
      <div class="metric"><div class="k">CINR</div><div class="v" id="cinr">--</div></div>
      <div class="metric"><div class="k">Max RSSI</div><div class="v" id="maxRssi">--</div></div>
      <div class="metric"><div class="k">Delta from max</div><div class="v" id="delta">--</div></div>
    </div>
    <div class="panel">
      <div class="hint" id="hint">awaiting movement history</div>
      <div class="sub" id="status">awaiting collector</div>
      <div style="display:flex; gap:10px; margin-top:12px; flex-wrap:wrap;">
        <button id="motionBtn">Enable motion</button>
        <button id="toneBtn">Tone off</button>
        <button id="resetBtn">Reset peak</button>
        <button id="reloginBtn">Relogin radio</button>
        <button id="pauseBtn">Pause polling</button>
      </div>
    </div>
  </div>
  <script>
    const ip = {safe_ip!r};
    const pollMs = {poll_ms};
    const bubble = document.getElementById("bubble");
    const peak = document.getElementById("peak");
    const rssiEl = document.getElementById("rssi");
    const cinrEl = document.getElementById("cinr");
    const maxRssiEl = document.getElementById("maxRssi");
    const deltaEl = document.getElementById("delta");
    const hintEl = document.getElementById("hint");
    const statusEl = document.getElementById("status");
    const motionBtn = document.getElementById("motionBtn");
    const toneBtn = document.getElementById("toneBtn");
    const resetBtn = document.getElementById("resetBtn");
    const reloginBtn = document.getElementById("reloginBtn");
    const pauseBtn = document.getElementById("pauseBtn");
    const sourceBadge = document.getElementById("sourceBadge");
    const freshBadge = document.getElementById("freshBadge");
    const banner = document.getElementById("banner");

    let latest = null;
    let movement = {{ beta: 0, gamma: 0 }};
    let peakMovement = {{ beta: 0, gamma: 0 }};
    let audioCtx = null;
    let oscillator = null;
    let toneOn = false;
    let paused = false;
    let lastPeakRssi = null;
    let chirpGain = null;

    function fmt(value, unit="") {{
      return value == null || Number.isNaN(value) ? "--" : `${{Number(value).toFixed(1)}}${{unit}}`;
    }}

    function clamp(v, min, max) {{
      return Math.min(max, Math.max(min, v));
    }}

    function place(el, beta, gamma, radius=118) {{
      const x = clamp(gamma / 45, -1, 1) * radius;
      const y = clamp(beta / 45, -1, 1) * radius;
      el.style.transform = `translate(calc(-50% + ${{x}}px), calc(-50% + ${{y}}px))`;
    }}

    function freshness(updatedAt) {{
      if (!updatedAt) return ["warn", "waiting"];
      const age = Date.now() / 1000 - Number(updatedAt);
      if (age < 2.5) return ["ok", `fresh ${age.toFixed(1)}s`];
      if (age < 6) return ["warn", `stale ${age.toFixed(1)}s`];
      return ["bad", `old ${age.toFixed(1)}s`];
    }}

    async function enableMotion() {{
      try {{
        if (typeof DeviceOrientationEvent !== "undefined" && typeof DeviceOrientationEvent.requestPermission === "function") {{
          const state = await DeviceOrientationEvent.requestPermission();
          if (state !== "granted") throw new Error("Motion permission denied");
        }}
        window.addEventListener("deviceorientation", (event) => {{
          movement = {{ beta: event.beta || 0, gamma: event.gamma || 0 }};
          place(bubble, movement.beta, movement.gamma);
        }});
        motionBtn.textContent = "Motion enabled";
      }} catch (err) {{
        statusEl.textContent = `motion unavailable: ${{err}}`;
      }}
    }}

    function ensureAudio() {{
      if (!audioCtx) {{
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
        oscillator = audioCtx.createOscillator();
        chirpGain = audioCtx.createGain();
        const gain = audioCtx.createGain();
        gain.gain.value = 0.03;
        chirpGain.gain.value = 0;
        oscillator.type = "sine";
        oscillator.connect(gain);
        oscillator.connect(chirpGain);
        gain.connect(audioCtx.destination);
        chirpGain.connect(audioCtx.destination);
        oscillator.start();
      }}
    }}

    function chirpNewPeak() {{
      ensureAudio();
      if (!audioCtx || !chirpGain) return;
      const now = audioCtx.currentTime;
      chirpGain.gain.cancelScheduledValues(now);
      chirpGain.gain.setValueAtTime(0.001, now);
      chirpGain.gain.exponentialRampToValueAtTime(0.05, now + 0.02);
      chirpGain.gain.exponentialRampToValueAtTime(0.001, now + 0.18);
      oscillator.frequency.setValueAtTime(720, now);
      oscillator.frequency.exponentialRampToValueAtTime(1100, now + 0.16);
    }}

    function updateTone() {{
      if (!toneOn || !latest || latest.current?.current_rssi == null) return;
      ensureAudio();
      const base = 220;
      const current = Number(latest.current.current_rssi);
      const peakRssi = latest.max_rssi_session == null ? current : Number(latest.max_rssi_session);
      const delta = current - peakRssi;
      const freq = base + Math.max(0, 200 + delta * 18);
      oscillator.frequency.setValueAtTime(freq, audioCtx.currentTime);
    }}

    async function poll() {{
      if (paused) {{
        statusEl.textContent = "polling paused";
        setTimeout(poll, pollMs);
        return;
      }}
      try {{
        const resp = await fetch(`/v1/siklu/${{ip}}/alignment?poll_ms=${{pollMs}}`, {{ cache: "no-store" }});
        latest = await resp.json();
        const current = latest.current || {{}};
        const [freshClass, freshText] = freshness(latest.updated_at);
        const source = current.source || "unknown";
        const error = current.collector_error || current.webui_error || "";
        rssiEl.textContent = fmt(current.current_rssi, " dBm");
        cinrEl.textContent = fmt(current.current_cinr, " dB");
        maxRssiEl.textContent = fmt(latest.max_rssi_session, " dBm");
        deltaEl.textContent = fmt(latest.delta_from_max_rssi, " dB");
        hintEl.textContent = String(latest.peak_hint || "insufficient_history").replaceAll("_", " ");
        const peakAt = latest.peak_hold_at ? new Date(Number(latest.peak_hold_at) * 1000).toLocaleTimeString() : "--";
        statusEl.textContent = `${{current.name || ip}} · ${{current.status || "unknown"}} · ${{current.source || "unknown"}} · peak ${peakAt}`;
        sourceBadge.className = `badge ${{source === "webui" ? "ok" : source === "artifact" ? "warn" : "bad"}}`;
        sourceBadge.textContent = `source ${{source}}`;
        freshBadge.className = `badge ${{freshClass}}`;
        freshBadge.textContent = freshText;
        banner.className = `banner ${{error ? "bad" : freshClass === "bad" ? "warn" : "warn"}}`;
        banner.textContent = error || (freshClass === "ok" ? "Live telemetry OK." : `Collector connected but sample freshness is ${freshText}.`);
        if (latest.max_rssi_session != null && current.current_rssi != null && Number(current.current_rssi) >= Number(latest.max_rssi_session)) {{
          peakMovement = {{ ...movement }};
          place(peak, peakMovement.beta, peakMovement.gamma);
        }}
        if (latest.max_rssi_session != null && (lastPeakRssi == null || Number(latest.max_rssi_session) > Number(lastPeakRssi) + 0.01)) {{
          if (lastPeakRssi != null) chirpNewPeak();
          lastPeakRssi = Number(latest.max_rssi_session);
        }}
        updateTone();
      }} catch (err) {{
        statusEl.textContent = `poll error: ${{err}}`;
        banner.className = "banner bad";
        banner.textContent = `poll error: ${{err}}`;
      }}
      setTimeout(poll, pollMs);
    }}

    motionBtn.addEventListener("click", enableMotion);
    resetBtn.addEventListener("click", async () => {{
      try {{
        const resp = await fetch(`/v1/siklu/${{ip}}/reset-peak?poll_ms=${{pollMs}}`, {{ method: "POST" }});
        latest = await resp.json();
        lastPeakRssi = latest.max_rssi_session;
        peakMovement = {{ ...movement }};
        place(peak, peakMovement.beta, peakMovement.gamma);
        statusEl.textContent = `peak reset at current position`;
      }} catch (err) {{
        statusEl.textContent = `reset failed: ${{err}}`;
      }}
    }});
    reloginBtn.addEventListener("click", async () => {{
      try {{
        banner.className = "banner warn";
        banner.textContent = "Resetting backend radio session and forcing relogin...";
        await fetch(`/v1/siklu/${{ip}}/reset-session?poll_ms=${{pollMs}}`, {{ method: "POST" }});
      }} catch (err) {{
        banner.className = "banner bad";
        banner.textContent = `session reset failed: ${{err}}`;
      }}
    }});
    pauseBtn.addEventListener("click", () => {{
      paused = !paused;
      pauseBtn.textContent = paused ? "Resume polling" : "Pause polling";
      if (!paused) poll();
    }});
    toneBtn.addEventListener("click", async () => {{
      toneOn = !toneOn;
      toneBtn.textContent = toneOn ? "Tone on" : "Tone off";
      if (toneOn) ensureAudio();
      if (toneOn && audioCtx && audioCtx.state === "suspended") {{
        await audioCtx.resume();
      }}
      updateTone();
    }});
    poll();
  </script>
</body>
</html>"""


@router.get("/siklu-link/{ip_a}/{ip_b}", response_class=HTMLResponse)
def siklu_link_page(ip_a: str, ip_b: str, poll_ms: int = Query(default=750, ge=250, le=1000)) -> str:
    a = str(ip_a).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    b = str(ip_b).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>Siklu Link {a} ↔ {b}</title>
  <style>
    body {{
      margin: 0;
      background: #050b12;
      color: #eef7ff;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }}
    .app {{ padding: 16px; display: grid; gap: 14px; }}
    .header, .panel {{
      border: 1px solid rgba(103, 203, 255, 0.26);
      background: rgba(9, 21, 32, 0.88);
      border-radius: 16px;
      padding: 16px;
    }}
    .title {{ font-size: 28px; font-weight: 800; }}
    .sub {{ color: #91a8bb; font-size: 13px; margin-top: 6px; }}
    .grid {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }}
    .k {{ color:#91a8bb; font-size:12px; }}
    .v {{ font-size:26px; font-weight:800; margin-top:4px; }}
    .row {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 10px; margin-top: 10px; }}
    .metric {{ background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.05); border-radius: 12px; padding: 10px; }}
    .warn {{ color:#ffb347; }}
    .bad {{ color:#ff6b6b; }}
    .ok {{ color:#67ff8e; }}
    button {{
      border: 1px solid rgba(103, 203, 255, 0.26);
      background: rgba(103, 203, 255, 0.08);
      color: #eef7ff;
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
    }}
    a {{ color:#67cbff; }}
    textarea {{
      width: 100%;
      min-height: 90px;
      background: rgba(255,255,255,0.03);
      color: #eef7ff;
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      padding: 10px;
      font: inherit;
      box-sizing: border-box;
    }}
  </style>
</head>
<body>
  <div class="app">
    <div class="header">
      <div class="title">Siklu Link View</div>
      <div class="sub">{a} ↔ {b} · poll {poll_ms}ms</div>
      <div class="sub"><a href="/siklu/{a}">open {a}</a> · <a href="/siklu/{b}">open {b}</a></div>
      <div class="sub" style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap;">
        <button id="reset-a">reset {a} peak</button>
        <button id="reset-b">reset {b} peak</button>
        <button id="reset-both">reset both peaks</button>
        <button id="relogin-a">relogin {a}</button>
        <button id="relogin-b">relogin {b}</button>
      </div>
      <div class="sub" id="shared-banner" style="margin-top:10px;">Waiting for samples.</div>
    </div>
    <div class="grid">
      <div class="panel" id="panel-a"></div>
      <div class="panel" id="panel-b"></div>
    </div>
    <div class="panel">
      <div class="k">Shared notes</div>
      <textarea id="shared-notes" placeholder="Call out best azimuth, best elevation, handoff notes, or pause points."></textarea>
      <div class="sub" id="notes-status">Notes unsaved.</div>
    </div>
  </div>
  <script>
    const ips = [{a!r}, {b!r}];
    const pollMs = {poll_ms};
    let paused = false;
    let notesDirty = false;
    let saveTimer = null;
    const sharedBanner = document.getElementById('shared-banner');
    const notesEl = document.getElementById('shared-notes');
    const notesStatus = document.getElementById('notes-status');
    function fmt(v, unit="") {{
      return v == null || Number.isNaN(v) ? "--" : `${{Number(v).toFixed(1)}}${{unit}}`;
    }}
    function ageClass(updatedAt) {{
      if (!updatedAt) return "bad";
      const age = Date.now()/1000 - Number(updatedAt);
      if (age < 2.5) return "ok";
      if (age < 6) return "warn";
      return "bad";
    }}
    function render(panelId, ip, data) {{
      const panel = document.getElementById(panelId);
      const cur = data.current || {{}};
      const freshness = ageClass(data.updated_at);
      const error = cur.collector_error || cur.webui_error || "";
      const peakAt = data.peak_hold_at ? new Date(Number(data.peak_hold_at) * 1000).toLocaleTimeString() : "--";
      panel.innerHTML = `
        <div class="title" style="font-size:22px">${{ip}}</div>
        <div class="sub">${{cur.source || 'unknown'}} · <span class="${{freshness}}">${{cur.status || 'unknown'}}</span></div>
        <div class="row">
          <div class="metric"><div class="k">RSSI</div><div class="v">${{fmt(cur.current_rssi, ' dBm')}}</div></div>
          <div class="metric"><div class="k">CINR</div><div class="v">${{fmt(cur.current_cinr, ' dB')}}</div></div>
          <div class="metric"><div class="k">Session max RSSI</div><div class="v">${{fmt(data.max_rssi_session, ' dBm')}}</div></div>
          <div class="metric"><div class="k">Delta from max</div><div class="v">${{fmt(data.delta_from_max_rssi, ' dB')}}</div></div>
        </div>
        <div class="sub" style="margin-top:10px">alignment=${{cur.alignment_status || 'unknown'}} · mode=${{cur.rf_mode || 'unknown'}} · hint=${{data.peak_hint || 'unknown'}} · peak=${{peakAt}}</div>
        <div class="sub ${{error ? 'bad' : freshness}}" style="margin-top:6px">${{error || `sample ${freshness}`}}</div>
      `;
    }}
    function updateSharedBanner(results) {{
      const errors = results.map((r) => r?.current?.collector_error || r?.current?.webui_error).filter(Boolean);
      const stale = results.some((r) => ageClass(r?.updated_at) !== 'ok');
      if (errors.length) {{
        sharedBanner.className = 'sub bad';
        sharedBanner.textContent = errors.join(' | ');
        return;
      }}
      if (stale) {{
        sharedBanner.className = 'sub warn';
        sharedBanner.textContent = 'One or both radios are stale. Check source badges and relogin if needed.';
        return;
      }}
      sharedBanner.className = 'sub ok';
      sharedBanner.textContent = 'Both radios are live and fresh.';
    }}
    async function loadNotes() {{
      const resp = await fetch(`/v1/siklu-link/${{ips[0]}}/${{ips[1]}}/notes`, {{ cache: 'no-store' }});
      const data = await resp.json();
      notesEl.value = data.text || '';
      notesStatus.textContent = data.updated_at ? `Notes saved ${{new Date(Number(data.updated_at) * 1000).toLocaleTimeString()}}` : 'Notes empty.';
      notesDirty = false;
    }}
    async function saveNotes() {{
      saveTimer = null;
      const resp = await fetch(`/v1/siklu-link/${{ips[0]}}/${{ips[1]}}/notes`, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ text: notesEl.value }})
      }});
      const data = await resp.json();
      notesStatus.textContent = `Notes saved ${{new Date(Number(data.updated_at) * 1000).toLocaleTimeString()}}`;
      notesDirty = false;
    }}
    function queueSaveNotes() {{
      notesDirty = true;
      notesStatus.textContent = 'Saving notes...';
      if (saveTimer) clearTimeout(saveTimer);
      saveTimer = setTimeout(saveNotes, 500);
    }}
    async function tick() {{
      if (paused) {{
        setTimeout(tick, pollMs);
        return;
      }}
      const results = [];
      for (const [idx, ip] of ips.entries()) {{
        try {{
          const resp = await fetch(`/v1/siklu/${{ip}}/alignment?poll_ms=${{pollMs}}`, {{ cache: 'no-store' }});
          const data = await resp.json();
          results.push(data);
          render(idx === 0 ? 'panel-a' : 'panel-b', ip, data);
        }} catch (err) {{
          const failed = {{ current: {{ collector_error: String(err), source: 'browser' }} }};
          results.push(failed);
          render(idx === 0 ? 'panel-a' : 'panel-b', ip, failed);
        }}
      }}
      updateSharedBanner(results);
      setTimeout(tick, pollMs);
    }}
    async function resetPeak(ip) {{
      await fetch(`/v1/siklu/${{ip}}/reset-peak?poll_ms=${{pollMs}}`, {{ method: 'POST' }});
    }}
    async function resetSession(ip) {{
      await fetch(`/v1/siklu/${{ip}}/reset-session?poll_ms=${{pollMs}}`, {{ method: 'POST' }});
    }}
    document.getElementById('reset-a').addEventListener('click', () => resetPeak(ips[0]));
    document.getElementById('reset-b').addEventListener('click', () => resetPeak(ips[1]));
    document.getElementById('reset-both').addEventListener('click', async () => {{
      await Promise.all(ips.map(resetPeak));
    }});
    document.getElementById('relogin-a').addEventListener('click', () => resetSession(ips[0]));
    document.getElementById('relogin-b').addEventListener('click', () => resetSession(ips[1]));
    const pause = document.createElement('button');
    pause.id = 'pause-all';
    pause.textContent = 'Pause polling';
    pause.addEventListener('click', () => {{
      paused = !paused;
      pause.textContent = paused ? 'Resume polling' : 'Pause polling';
      if (!paused) tick();
    }});
    document.querySelector('.header .sub[style]').appendChild(pause);
    notesEl.addEventListener('input', queueSaveNotes);
    loadNotes();
    tick();
  </script>
</body>
</html>"""
