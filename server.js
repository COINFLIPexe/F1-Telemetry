const http = require('http'), fs = require('fs'), path = require('path'), zlib = require('zlib');
const WebSocket = require('ws');
const BASE = 'livetiming.formula1.com/signalr';
const HUB = encodeURIComponent('[{"name":"Streaming"}]');
let circuitVer = 0, liveRain = null, prog = 0, carData = {}, geo = null, geoKey = '', circuit = null, circuitKey = '', state = {}, standings = {}, connected = false, cars = {}, track = [], seen = new Set();

function merge(t, s) {
  for (const k in s) {
    if (s[k] && typeof s[k] === 'object' && !Array.isArray(s[k])) {
      if (!t[k] || typeof t[k] !== 'object') t[k] = {};
      merge(t[k], s[k]);
    } else t[k] = s[k];
  }
  return t;
}
function ingest(topic, v) {
  if (topic.endsWith('.z')) {
    try { v = JSON.parse(zlib.inflateRawSync(Buffer.from(v, 'base64')).toString()); } catch { return; }
    if (topic === 'CarData.z') {
      const e = (v.Entries || []).pop();
      if (e) for (const n in e.Cars || {}) { const c = e.Cars[n].Channels || {}; const o = carData[n] || {}; carData[n] = { s: c[2] != null ? c[2] : o.s, g: c[3] != null ? c[3] : o.g, d: c[45] != null ? c[45] : o.d }; }
      return;
    }
    if (topic === 'Position.z') {
      (v.Position || []).forEach(p => {
        for (const n in p.Entries || {}) {
          const e = p.Entries[n];
          if (e.Status === 'OffTrack' || (!e.X && !e.Y)) continue;
          cars[n] = { x: e.X, y: e.Y };
          const k = Math.round(e.X / 180) + ',' + Math.round(e.Y / 180);
          if (!seen.has(k) && track.length < 4000) { seen.add(k); track.push([e.X, e.Y]); }
        }
      });
    }
    return;
  }
  merge(state[topic] = state[topic] || {}, v);
  if (topic === 'SessionInfo') { loadCircuit(); loadGeo(); }
}
async function loadRainNow() {
  if (!geo) return;
  try { const j = await (await fetch(`https://api.open-meteo.com/v1/forecast?latitude=${geo.lat}&longitude=${geo.lon}&current=precipitation`)).json(); liveRain = { mm: j.current.precipitation }; } catch (e) {}
}
setInterval(loadRainNow, 180000);
const liveFlag = () => ({ code: { 2: 'Y', 4: 'SC', 5: 'RED', 6: 'VSC', 7: 'VSCE' }[(state.TrackStatus || {}).Status] || '', txt: '' });
async function loadGeo() {
  const m = (state.SessionInfo || {}).Meeting || {}, k = (m.Location || '') + '|' + ((m.Country || {}).Name || '');
  if (!m.Location || geoKey === k) return;
  geoKey = k;
  for (const q of [m.Location, (m.Country || {}).Name]) {
    if (!q) continue;
    try {
      const j = await (await fetch('https://geocoding-api.open-meteo.com/v1/search?count=1&name=' + encodeURIComponent(q))).json();
      if (j.results && j.results[0]) { geo = { lat: j.results[0].latitude, lon: j.results[0].longitude }; loadRainNow(); loadRainReplay(); return; }
    } catch (e) {}
  }
}
async function loadCircuit() {
  const s = state.SessionInfo || {}, key = ((s.Meeting || {}).Circuit || {}).Key, yr = (s.StartDate || '').slice(0, 4);
  if (!key || !yr || circuitKey === key + yr) return;
  circuitKey = key + yr;
  try {
    const j = await (await fetch(`https://api.multiviewer.app/api/v1/circuits/${key}/${yr}`, { headers: { 'User-Agent': 'f1-live' } })).json();
    if (j.x && j.y) { circuit = j.x.map((x, i) => [x, j.y[i]]); circuitVer++; }
  } catch (e) { circuitKey = ''; }
}

async function connect() {
  try {
    const r = await fetch(`https://${BASE}/negotiate?connectionData=${HUB}&clientProtocol=1.5`);
    const j = await r.json();
    const cookie = (r.headers.getSetCookie ? r.headers.getSetCookie() : []).map(c => c.split(';')[0]).join('; ');
    const ws = new WebSocket(
      `wss://${BASE}/connect?clientProtocol=1.5&transport=webSockets&connectionToken=${encodeURIComponent(j.ConnectionToken)}&connectionData=${HUB}`,
      { headers: { 'User-Agent': 'BestHTTP', 'Accept-Encoding': 'gzip,identity', Cookie: cookie } });
    ws.on('open', () => {
      connected = true; state = {}; cars = {}; carData = {};
      ws.send(JSON.stringify({ H: 'Streaming', M: 'Subscribe', I: 1,
        A: [['Heartbeat', 'SessionInfo', 'TrackStatus', 'DriverList', 'TimingData', 'TimingAppData', 'LapCount', 'SessionStatus', 'WeatherData', 'ExtrapolatedClock', 'CarData.z', 'Position.z']] }));
    });
    ws.on('message', m => {
      let d; try { d = JSON.parse(m); } catch { return; }
      if (d.R) for (const k in d.R) ingest(k, d.R[k]);
      (d.M || []).forEach(x => { if (x.M === 'feed') ingest(x.A[0], x.A[1]); });
    });
    ws.on('close', () => { connected = false; setTimeout(connect, 5000); });
    ws.on('error', () => { try { ws.close(); } catch {} });
  } catch (e) { setTimeout(connect, 10000); }
}

async function loadStandings() {
  try {
    const r = await fetch('https://api.jolpi.ca/ergast/f1/current/driverStandings.json');
    const j = await r.json();
    const o = {};
    j.MRData.StandingsTable.StandingsLists[0].DriverStandings.forEach(x => o[x.Driver.code] = +x.points);
    standings = o;
  } catch (e) {}
}

const last = o => { const k = Object.keys(o || {}).sort((a, b) => a - b).pop(); return k === undefined ? {} : o[k]; };
function snapshot() {
  const dl = state.DriverList || {}, tl = (state.TimingData || {}).Lines || {}, ap = (state.TimingAppData || {}).Lines || {}, s = state.SessionInfo || {};
  const rows = Object.entries(tl).map(([n, l]) => {
    const secs = [0, 1, 2].map(i => { const x = (l.Sectors || {})[i] || {}; return { v: x.Value || '', c: x.OverallFastest ? 2 : x.PersonalFastest ? 1 : 0 }; });
    const segs = [0, 1, 2].map(i => { const sg = ((l.Sectors || {})[i] || {}).Segments || {}; return Object.keys(sg).sort((a, b) => a - b).map(k => (sg[k] || {}).Status | 0); });
    const sp = l.Speeds || {}, st = last((ap[n] || {}).Stints);
    return { tla: (dl[n] || {}).Tla || n, color: '#' + ((dl[n] || {}).TeamColour || '888888'), pos: +l.Position || 99,
      gap: l.GapToLeader || l.TimeDiffToFastest || '', int: (l.IntervalToPositionAhead || {}).Value || '',
      last: (l.LastLapTime || {}).Value || '', best: (l.BestLapTime || {}).Value || '', pit: !!l.InPit,
      tyre: (st.Compound || '')[0] || '', tl: st.TotalLaps | 0, gear: (carData[n] || {}).g, spd: (carData[n] || {}).s, ovt: ((carData[n] || {}).d | 0) >= 10, ret: !!l.Retired, secs, segs, out: !!l.KnockedOut, st: +((sp.ST || {}).Value) || 0 };
  }).sort((a, b) => a.pos - b.pos);
  const cl = Object.entries(cars).map(([n, c]) => ({ tla: (dl[n] || {}).Tla || n, color: '#' + ((dl[n] || {}).TeamColour || '888888'), x: c.x, y: c.y }));
  return { connected, hasData: rows.length > 0, session: s.Name || '', meeting: (s.Meeting || {}).Name || '',
    track: (state.TrackStatus || {}).Message || '', rows, cars: cl, standings,
    part: (state.TimingData || {}).SessionPart || 0, lap: (state.LapCount || {}).CurrentLap || 0,
    total: (state.LapCount || {}).TotalLaps || 0, wx: state.WeatherData || {}, clock: state.ExtrapolatedClock || {}, gmt: s.GmtOffset || '', geo, trk: circuitVer, flag: liveFlag(), rain: liveRain, status: (state.SessionStatus || {}).Status || '' };
}

// ---------- REPLAY (OpenF1, sessioni passate) ----------
const OF = 'https://api.openf1.org/v1';
const iso = ms => new Date(ms).toISOString();
let rq = Promise.resolve(), stamps = [], lastReq = 0, sessList = null;
let R = { on: false, loading: false, err: '', playing: false, speed: 1 };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ts = x => Date.parse(x);
const hms = ms => { ms = Math.max(0, ms / 1000 | 0); return [ms / 3600 | 0, ms % 3600 / 60 | 0, ms % 60].map(x => String(x).padStart(2, '0')).join(':'); };
const fmtLap = s => { const m = s / 60 | 0, r = (s - m * 60).toFixed(3); return m ? m + ':' + r.padStart(6, '0') : r; };
function at(a, t) { let lo = 0, hi = a.length - 1, r = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (a[m].t <= t) { r = m; lo = m + 1; } else hi = m - 1; } return r; }

// rispetta il limite gratuito di OpenF1 (3 al secondo, 30 al minuto)
function of1(p, qs) {
  const run = async () => {
    for (let k = 0; k < 6; k++) {
      const n = Date.now(); stamps = stamps.filter(x => n - x < 60000);
      const w = Math.max(stamps.length >= 27 ? stamps[0] + 60000 - n : 0, lastReq + 450 - n);
      if (w > 0) await sleep(w);
      stamps.push(Date.now()); lastReq = Date.now();
      const r = await fetch(`${OF}/${p}?${qs}`);
      if (r.status === 429) { await sleep(3000); continue; }
      if (r.status === 404) return [];
      if (!r.ok) throw new Error(p + ' ' + r.status);
      return r.json();
    }
    throw new Error(p + ': troppe richieste');
  };
  const pr = rq.then(run); rq = pr.catch(() => {}); return pr;
}

async function listSessions() {
  if (sessList && Date.now() - sessList.t < 600000) return sessList.v;
  let a = await of1('sessions', 'year=' + new Date().getFullYear());
  if (!a.length) a = await of1('sessions', 'year=' + (new Date().getFullYear() - 1));
  const v = a.filter(x => Date.parse(x.date_end) < Date.now()).sort((x, y) => Date.parse(y.date_start) - Date.parse(x.date_start))
    .map(x => ({ key: x.session_key, mk: x.meeting_key, country: x.country_name, loc: x.location, name: x.session_name, date: x.date_start.slice(0, 10), ts: Date.parse(x.date_start), label: `${x.country_name} · ${x.session_name} · ${x.date_start.slice(0, 10)}` }));
  sessList = { t: Date.now(), v }; return v;
}

async function loadReplay(key) {
  R = { on: false, loading: true, err: '', playing: false, speed: 1 };
  try {
    prog = 0;
    const [s] = await of1('sessions', 'session_key=' + key); prog++;
    const g = async p => { const r = await of1(p, 'session_key=' + key); prog++; return r; };
    const drv = await g('drivers'), laps = await g('laps'), pos = await g('position'), st = await g('stints'), wx = await g('weather');
    let iv = [], pit = [], rc = [];
    try { rc = await g('race_control'); } catch (e) {}
    try { iv = await g('intervals'); } catch (e) {}
    try { pit = await g('pit'); } catch (e) {}
    const D = {}; drv.forEach(d => D[d.driver_number] = { tla: d.name_acronym, color: '#' + (d.team_colour || '888888') });
    const by = (arr, f) => { const o = {}; arr.forEach(x => (o[x.driver_number] = o[x.driver_number] || []).push(f(x))); for (const k in o) o[k].sort((a, b) => a.t - b.t); return o; };
    const P = by(pos, x => ({ t: ts(x.date), v: x.position }));
    const I = by(iv, x => ({ t: ts(x.date), g: x.gap_to_leader, i: x.interval }));
    const PT = by(pit, x => ({ t: ts(x.date), d: (x.lane_duration || x.stop_duration || x.pit_duration || 25) * 1000, lap: x.lap_number }));
    const S = by(st, x => ({ t: x.lap_start || 0, c: x.compound, a: x.tyre_age_at_start || 0 }));
    const lapsByD = {}; laps.forEach(x => { (lapsByD[x.driver_number] = lapsByD[x.driver_number] || {})[x.lap_number] = x; });
    laps.forEach(x => { if (!x.date_start && x.lap_number === 1) { const l2 = (lapsByD[x.driver_number] || {})[2];
      x.date_start = l2 && l2.date_start ? (x.lap_duration ? iso(ts(l2.date_start) - x.lap_duration * 1000) : iso(ts(l2.date_start) - 100000)) : s.date_start; } });
    const L = by(laps.filter(x => x.date_start), x => { const t = ts(x.date_start);
      return { t, n: x.lap_number, end: t + (x.lap_duration || 0) * 1000, dur: x.lap_duration,
        s: [x.duration_sector_1, x.duration_sector_2, x.duration_sector_3], seg: [x.segments_sector_1, x.segments_sector_2, x.segments_sector_3], st: x.st_speed }; });
    let maxLap = 0; const starts = [];
    for (const n in L) {
      let b = 1e9, pb = [1e9, 1e9, 1e9], sp = 0;
      L[n].forEach(l => { maxLap = Math.max(maxLap, l.n); starts.push(l.t); if (l.dur) b = Math.min(b, l.dur);
        l.best = b < 1e9 ? b : null; pb = pb.map((v, i) => l.s[i] ? Math.min(v, l.s[i]) : v); l.pb = pb; if (l.st) sp = l.st; l.sp = sp; });
    }
    starts.sort((a, b) => a - b);
    const parts = starts.length ? [starts[0]] : [];
    for (let i = 1; i < starts.length; i++) if (starts[i] - starts[i - 1] > 240000) parts.push(starts[i]);
    const RC = rc.map(x => ({ t: ts(x.date), cat: x.category, msg: String(x.message || '').toUpperCase(), flag: x.flag, scope: x.scope, sector: x.sector })).sort((a, b) => a.t - b.t);
    const W = wx.map(x => Object.assign({ t: ts(x.date) }, x)).sort((a, b) => a.t - b.t);
    R = { on: true, loading: false, err: '', playing: false, speed: 1, key, s, D, P, I, PT, S, L, W, RC, parts, maxLap,
      race: /^(Race|Sprint)$/.test(s.session_name), t0: ts(s.date_start), t1: ts(s.date_end), t: ts(s.date_start), last: Date.now(),
      buf: { a: 0, b: 0, loc: {}, car: {}, busy: false } };
    circuit = null; circuitKey = ''; geo = null; geoKey = ''; circuitVer++;
    state.SessionInfo = { Name: s.session_name, StartDate: s.date_start, GmtOffset: s.gmt_offset,
      Meeting: { Location: s.location, Country: { Name: s.country_name }, Circuit: { Key: s.circuit_key } } };
    loadCircuit(); loadGeo();
  } catch (e) { R = { on: false, loading: false, err: String(e.message || e), playing: false, speed: 1 }; }
}

// scarica 12 secondi di posizioni e telemetria di tutte le auto in 2 richieste
async function pull(T) {
  const b = R.buf; if (b.busy) return; b.busy = true;
  const a = (T >= b.a && T < b.b) ? b.b : T, e = a + 12000, f = d => new Date(d).toISOString();
  const win = `session_key=${R.key}&date>=${f(a)}&date<${f(e)}`;
  try {
    const lo = await of1('location', win), ca = await of1('car_data', win);
    const grp = (arr, fn) => { const o = {}; arr.forEach(x => (o[x.driver_number] = o[x.driver_number] || []).push(Object.assign({ t: ts(x.date) }, fn(x)))); for (const k in o) o[k].sort((p, q) => p.t - q.t); return o; };
    b.loc = grp(lo, x => ({ x: x.x, y: x.y })); b.car = grp(ca, x => ({ s: x.speed, g: x.n_gear, d: x.drs }));
    b.a = a; b.b = e;
  } catch (e2) {}
  b.busy = false;
}

async function loadRainReplay() {
  if (!R.on || !geo) return;
  const d = new Date(R.t0).toISOString().slice(0, 10);
  for (const base of ['https://archive-api.open-meteo.com/v1/archive', 'https://api.open-meteo.com/v1/forecast']) {
    try {
      const j = await (await fetch(`${base}?latitude=${geo.lat}&longitude=${geo.lon}&start_date=${d}&end_date=${d}&hourly=precipitation&timezone=UTC`)).json();
      if (j.hourly && j.hourly.precipitation && j.hourly.precipitation.some(x => x != null)) { R.rain = j.hourly.precipitation; return; }
    } catch (e) {}
  }
}
function flagAt(T) {
  let sc = '', red = false, cheq = false; const y = new Set();
  for (const e of R.RC) {
    if (e.t > T) break;
    if (e.cat === 'SafetyCar') {
      if (e.msg.includes('VIRTUAL SAFETY CAR DEPLOYED')) sc = 'VSC'; else if (e.msg.includes('VIRTUAL SAFETY CAR ENDING')) sc = 'VSCE';
      else if (e.msg.includes('SAFETY CAR DEPLOYED')) sc = 'SC'; else if (e.msg.includes('IN THIS LAP')) sc = 'SCE';
    } else if (e.cat === 'Flag') {
      if (e.flag === 'RED') red = true; else if (e.flag === 'CHEQUERED') cheq = true;
      else if ((e.flag === 'GREEN' || e.flag === 'CLEAR') && e.scope === 'Track') { red = false; sc = ''; y.clear(); }
      else if (/YELLOW/.test(e.flag || '') && e.scope === 'Sector') y.add(e.sector);
      else if (e.flag === 'CLEAR' && e.scope === 'Sector') y.delete(e.sector);
    }
  }
  const code = cheq ? 'CHEQ' : red ? 'RED' : sc || (y.size ? 'Y' : '');
  return { code, txt: code === 'Y' ? [...y].map(x => 'S' + x).join(', ') : '' };
}
function advance() {
  const now = Date.now();
  if (R.on && R.playing) { R.t = Math.min(R.t1, R.t + (now - R.last) * R.speed); if (R.t >= R.t1) R.playing = false; }
  R.last = now;
}

function replaySnap() {
  advance();
  const T = R.t, b = R.buf;
  if (T < b.a || T > b.b - 5000) pull(T);
  const rows = [], cars = []; let maxLap = 0;
  const fm = v => v == null ? '' : typeof v === 'number' ? '+' + v.toFixed(3) : String(v);
  for (const n in R.D) {
    const d = R.D[n], P = R.P[n] || []; if (!P.length) continue; const pi = Math.max(0, at(P, T));
    const L = R.L[n] || [], ci = at(L, T); let li = ci;
    while (li >= 0 && !(L[li].dur && L[li].end <= T)) li--;
    const cur = ci >= 0 ? L[ci].n : 0; maxLap = Math.max(maxLap, cur);
    const lp = li >= 0 ? L[li] : null, I = R.I[n] || [], ii = at(I, T), iv = ii >= 0 ? I[ii] : null;
    const sg = R.S[n] || [], si = Math.max(sg.length ? 0 : -1, at(sg, cur || 1)), sti = si >= 0 ? sg[si] : null;
    const car = R.buf.car[n] || [], cj = at(car, T), c = cj >= 0 ? car[cj] : null;
    const lc = R.buf.loc[n] || [], lj = at(lc, T);
    const cl = ci >= 0 ? L[ci] : null, el = cl ? T - cl.t : 0; let acc = 0, ok = !!cl;
    const dn = [0, 1, 2].map(k => { if (!ok || !cl.s[k]) { ok = false; return false; } acc += cl.s[k] * 1000; return el >= acc; });
    const src = [0, 1, 2].map(k => dn[k] ? cl : lp);
    if (lj >= 0) cars.push({ tla: d.tla, color: d.color, x: lc[lj].x, y: lc[lj].y });
    rows.push({ tla: d.tla, color: d.color, pos: P[pi].v, gap: iv ? fm(iv.g) : '', int: iv ? fm(iv.i) : '',
      last: lp ? fmtLap(lp.dur) : '', best: lp && lp.best ? fmtLap(lp.best) : '', bestN: lp ? lp.best : null,
      pit: (R.PT[n] || []).some(p => { const o = L.find(l => l.n === p.lap + 1); const e = Math.max(p.t + p.d, o && o.t > p.t && o.t - p.t < 90000 ? o.t + 4000 : 0); return p.t <= T && T < e; }), tyre: sti ? (sti.c || '')[0] || '' : '', tl: sti ? Math.max(0, sti.a + cur - sti.t) : 0,
      gear: c ? c.g : undefined, spd: c ? c.s : undefined, ovt: !!(c && c.d >= 10),
      secs: [0, 1, 2].map(k => ({ v: src[k] && src[k].s[k] ? src[k].s[k].toFixed(3) : '', c: 0, raw: src[k] ? src[k].s[k] : null, pb: src[k] ? src[k].pb[k] : 1e9 })),
      segs: [0, 1, 2].map(k => { const a = dn[k] ? cl.seg[k] : null; return a ? a.map(x => x | 0) : new Array(((cl && cl.seg[k]) || (lp && lp.seg[k]) || []).length).fill(0); }),
      ls: ci >= 0 ? L[ci].t : 0, st: lp ? Math.round(lp.sp) || 0 : 0 });
  }
  rows.sort((a, b2) => a.pos - b2.pos);
  const anyLs = Math.max(0, ...rows.map(r => r.ls));
  rows.forEach(r => { r.ret = R.race && r.ls > 0 && anyLs - r.ls > 300000; delete r.ls; });
  const gb = [0, 1, 2].map(k => Math.min(...rows.map(r => r.secs[k].pb), 1e9));
  rows.forEach(r => r.secs.forEach((x, k) => { x.c = x.raw && x.raw === gb[k] ? 2 : x.raw && x.raw === x.pb ? 1 : 0; delete x.raw; delete x.pb; }));
  if (!R.race) rows.forEach((r, i) => { const b0 = rows[0].bestN, bp = i ? rows[i - 1].bestN : null;
    r.gap = i && r.bestN && b0 ? '+' + (r.bestN - b0).toFixed(3) : ''; r.int = i && r.bestN && bp ? '+' + (r.bestN - bp).toFixed(3) : ''; });
  const sn = R.s.session_name; let part = 0, rem = '';
  if (/Qualifying|Shootout/.test(sn) && R.parts.length) {
    part = Math.min(3, Math.max(1, R.parts.filter(x => x <= T + 60000).length));
    rem = hms((/Sprint/.test(sn) ? [12, 10, 8] : [18, 15, 12])[part - 1] * 60000 - (T - R.parts[part - 1]));
  } else if (!R.race) rem = hms(R.t1 - T);
  const w = R.W[at(R.W, T)] || {};
  return { connected: true, hasData: rows.length > 0, session: sn, meeting: R.s.location + ' (replay)', track: '', rows, cars, standings,
    part, lap: R.race ? Math.max(1, Math.min(maxLap, R.maxLap)) : 0, total: R.race ? R.maxLap : 0, status: '',
    wx: w.air_temperature == null ? {} : { AirTemp: String(w.air_temperature), TrackTemp: String(w.track_temperature), Humidity: String(w.humidity),
      WindSpeed: String(w.wind_speed), WindDirection: String(w.wind_direction), Rainfall: String(+w.rainfall ? 1 : 0) },
    clock: { rem }, gmt: R.s.gmt_offset || '', geo, now: T, trk: circuitVer, flag: flagAt(T),
    rain: R.rain ? { mm: R.rain[new Date(T).getUTCHours()] } : null };
}
const replayMeta = () => ({ on: R.on, loading: R.loading, err: R.err, playing: !!R.playing, speed: R.speed || 1, prog, progTotal: 9, frac: R.on ? (R.t - R.t0) / (R.t1 - R.t0) : 0 });
function replayCtl(p) {
  if (!R.on) return;
  advance();
  if (p.has('play')) R.playing = p.get('play') === '1';
  if (p.has('speed')) R.speed = Math.min(2, Math.max(1, +p.get('speed') || 1));
  if (p.has('pos')) R.t = R.t0 + (R.t1 - R.t0) * Math.min(1, Math.max(0, +p.get('pos')));
  if (p.has('lap')) { const n = +p.get('lap'); let m = Infinity; for (const k in R.L) R.L[k].forEach(l => { if (l.n === n && l.t < m) m = l.t; }); if (m < Infinity) R.t = m; }
  if (p.has('clock')) {
    const [h, mi] = p.get('clock').split(':').map(Number), mm = /^(-?)(\d+):(\d+)/.exec(R.s.gmt_offset || '0:0');
    const off = mm ? (mm[1] ? -1 : 1) * (mm[2] * 60 + +mm[3]) * 60000 : 0, loc = new Date(R.t0 + off);
    let tt = Date.UTC(loc.getUTCFullYear(), loc.getUTCMonth(), loc.getUTCDate(), h, mi) - off; if (tt < R.t0 - 3600000) tt += 86400000;
    R.t = Math.min(R.t1, Math.max(R.t0, tt));
  }
  if (p.has('off')) R.on = false;
}
// ---------- fine REPLAY ----------

http.createServer((req, res) => {
  const json = o => { res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }); res.end(JSON.stringify(o)); };
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/replay/sessions') return listSessions().then(json).catch(() => { res.writeHead(500); res.end('[]'); });
  if (u.pathname === '/replay/load') { loadReplay(u.searchParams.get('key')); return json({ ok: true }); }
  if (u.pathname === '/replay/ctl') { replayCtl(u.searchParams); return json({ ok: true }); }
  if (u.pathname === '/state') { const sn = R.on ? replaySnap() : snapshot(); sn.replay = replayMeta(); return json(sn); }
  if (req.url.startsWith('/track')) return json(circuit ? { line: true, pts: circuit } : { line: false, pts: track });
  fs.readFile(path.join(__dirname, 'index.html'), (e, b) => {
    res.writeHead(e ? 500 : 200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(b || 'errore');
  });
}).listen(process.env.PORT || 3000);

connect(); loadStandings(); setInterval(loadStandings, 10 * 60 * 1000);

if (process.env.F1_TEST) module.exports = { loadReplay, replaySnap, replayCtl, get R() { return R; } };
