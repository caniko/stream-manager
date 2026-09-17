const BG = [{type:"solid",color:"#14161A"}];
const PANEL = [{type:"solid",color:"#1D2126"}];
const PANEL2 = [{type:"solid",color:"#232930"}];
const ACCENT = [{type:"solid",color:"#4C8DFF"}];
const ACCENT_BG = [{type:"solid",color:"#22344F"}];
const TXT = [{type:"solid",color:"#E8EAED"}];
const MUT = [{type:"solid",color:"#9AA3AD"}];
const OK = [{type:"solid",color:"#3FB950"}];
const WARN = [{type:"solid",color:"#E5A63B"}];
const BAD = [{type:"solid",color:"#E5534B"}];

const root = I(null, {type:"frame", name:"Review 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
for (const t of ["Streams","Destinations","Review","History","Video"]) {
  const on = t === "Review";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:150, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
const snap = I(root, {type:"frame", name:"SnapshotBar", layout:"horizontal", width:"fill_container", height:40, gap:12, padding:[8,16], alignItems:"center", fill:[{type:"solid",color:"#1A2B1D"}]});
I(snap, {type:"text", content:"Reviewing immutable revision 4 of “Friday Build Stream” — edits made after this point need a fresh review.", fontSize:13, fontWeight:600, fill:OK});
const body = I(root, {type:"frame", name:"Body", layout:"vertical", width:"fill_container", height:"fill_container", gap:10, padding:[12,16]});

const revs = [
  ["youtube-main · PRIMARY","create liveBroadcast (autoStart off) → bind ingest → update snippet+status","Before: —  →  After: title + full description + 20:00 CEST","verified read-back","Apply",true],
  ["rumble-main · FUNNEL → YouTube","MANUAL: copy text → open dashboard → paste URL → record completion","Preview stream. Watch the full livestream on YouTube: <primary URL>","manual — not API verified","Open manual panel",true],
  ["kick-main · MIRROR","update channel now (separate confirm)","Before: “Old title”  →  After: “Friday Build Stream”","accepted, unverified","Apply",true],
  ["twitch-main · MIRROR","update channel now (separate confirm)","category + tags; no description endpoint; no off-platform ask","blocked: no separate confirm yet","Confirm channel-now",false]
];
for (const r of revs) {
  const card = I(body, {type:"frame", name:"Rev "+r[0], layout:"horizontal", width:"fill_container", height:118, gap:12, padding:[10,12], cornerRadius:10, fill:PANEL});
  const left = I(card, {type:"frame", name:"What", layout:"vertical", width:420, gap:4});
  I(left, {type:"text", content:r[0], fontSize:14, fontWeight:700, fill:TXT});
  I(left, {type:"text", content:r[1], fontSize:12, fill:MUT});
  I(left, {type:"text", content:"State: " + r[3], fontSize:12, fontWeight:600, fill: r[4]==="Apply" ? OK : WARN});
  const mid = I(card, {type:"frame", name:"Diff", layout:"vertical", width:"fill_container", gap:4, padding:[8,10], cornerRadius:8, fill:PANEL2});
  I(mid, {type:"text", content:r[2], fontSize:12, fill:TXT});
  const btn = I(card, {type:"frame", name:"GoBtn", layout:"horizontal", width:190, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: r[5] ? ACCENT : PANEL2});
  I(btn, {type:"text", content:r[4], fontSize:13, fontWeight:600, fill:TXT});
}
const foot = I(body, {type:"frame", name:"Foot", layout:"horizontal", width:"fill_container", height:34, gap:12, alignItems:"center"});
I(foot, {type:"text", content:"Cross-platform publishing is not atomic. Retries touch failed actions only.", fontSize:12, fill:MUT});
I(foot, {type:"text", content:"Policy: twitch-no-funnel-out ✓ · twitch-no-degrading-mask ✓", fontSize:12, fill:OK});
