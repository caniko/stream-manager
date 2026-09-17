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

const root = I(null, {type:"frame", name:"History 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
for (const t of ["Streams","Destinations","Review","History","Video"]) {
  const on = t === "History";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:150, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
const body = I(root, {type:"frame", name:"Body", layout:"vertical", width:"fill_container", height:"fill_container", gap:10, padding:[12,16]});
I(body, {type:"text", content:"Operation #41 · plan rev 4 · 2026-10-02 18:04 — retry affects failed actions only; successes are never replayed.", fontSize:13, fill:TXT});
const ops = [
  ["youtube-main","update snippet+status","applied · read-back verified","remote: abc123 · watch: youtube.com/watch?v=abc123","—",false,"ok"],
  ["rumble-main","manual copy → dashboard","manually completed (not API verified)","public URL recorded","—",false,"warn"],
  ["kick-main","update channel now","failed: 429 rate limited","retry after 60s","Retry",true,"bad"],
  ["twitch-main","update channel now","outcome unknown: timeout after dispatch","reconcile before retry","Reconcile",true,"warn"]
];
for (const o of ops) {
  const card = I(body, {type:"frame", name:"Op "+o[0], layout:"horizontal", width:"fill_container", height:96, gap:12, padding:[10,12], cornerRadius:10, alignItems:"center", fill:PANEL});
  const l = I(card, {type:"frame", name:"L", layout:"vertical", width:300, gap:3});
  I(l, {type:"text", content:o[0], fontSize:14, fontWeight:600, fill:TXT});
  I(l, {type:"text", content:o[1], fontSize:12, fill:MUT});
  const m = I(card, {type:"frame", name:"M", layout:"vertical", width:"fill_container", gap:3});
  I(m, {type:"text", content:o[2], fontSize:13, fontWeight:600, fill: o[6]==="ok" ? OK : o[6]==="warn" ? WARN : BAD});
  I(m, {type:"text", content:o[3], fontSize:12, fill:MUT});
  if (o[5]) {
    const b = I(card, {type:"frame", name:"Rb", layout:"horizontal", width:150, height:34, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:ACCENT});
    I(b, {type:"text", content:o[4], fontSize:13, fontWeight:600, fill:TXT});
  } else {
    const s = I(card, {type:"frame", name:"Done", layout:"horizontal", width:150, height:34, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:PANEL2});
    I(s, {type:"text", content:"done", fontSize:13, fill:MUT});
  }
}
I(body, {type:"text", content:"Remote IDs persist with the operation: retrying never creates duplicate broadcasts.", fontSize:12, fill:MUT});
