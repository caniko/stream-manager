const BG = [{type:"solid",color:"#14161A"}];
const PANEL = [{type:"solid",color:"#1D2126"}];
const PANEL2 = [{type:"solid",color:"#232930"}];
const ACCENT = [{type:"solid",color:"#4C8DFF"}];
const ACCENT_BG = [{type:"solid",color:"#22344F"}];
const TXT = [{type:"solid",color:"#E8EAED"}];
const MUT = [{type:"solid",color:"#9AA3AD"}];
const OK = [{type:"solid",color:"#3FB950"}];
const WARN = [{type:"solid",color:"#E5A63B"}];

const root = I(null, {type:"frame", name:"Destinations 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
for (const t of ["Streams","Destinations","Review","History","Video"]) {
  const on = t === "Destinations";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:150, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
I(root, {type:"text", content:"Transport (InstantClone declaration) and metadata authorization are separate states.", fontSize:13, fill:MUT});
const body = I(root, {type:"frame", name:"Body", layout:"vertical", width:"fill_container", height:"fill_container", gap:10, padding:[12,16]});
const probe = I(body, {type:"frame", name:"ProbeRow", layout:"horizontal", width:"fill_container", height:40, gap:12, alignItems:"center", padding:[8,12], cornerRadius:8, fill:PANEL});
I(probe, {type:"text", content:"Declared: 4 · source present: 4 · runtime/decryption: unknown (not probed)", fontSize:13, fill:TXT});
const rb = I(probe, {type:"frame", name:"RefreshBtn", layout:"horizontal", width:190, height:30, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:ACCENT});
I(rb, {type:"text", content:"Refresh transport status", fontSize:13, fontWeight:600, fill:TXT});

const rows = [
  ["youtube-main","YouTube","can-dev","declared · source ok","authorized (scopes ok)","update event","Set primary"],
  ["rumble-main","Rumble","—","declared · source ok","manual workflow","manual copy → dashboard","Open manual panel"],
  ["kick-main","Kick","can-dev","declared · source ok","authorized","update channel","Set primary"],
  ["twitch-main","Twitch","can-live","declared · source ok","authorized","update channel","Set primary"]
];
for (const r of rows) {
  const card = I(body, {type:"frame", name:"Row "+r[0], layout:"horizontal", width:"fill_container", height:84, gap:12, padding:[10,12], cornerRadius:10, alignItems:"center", fill:PANEL});
  const c1 = I(card, {type:"frame", name:"Id", layout:"vertical", width:240, gap:2});
  I(c1, {type:"text", content:r[0], fontSize:14, fontWeight:600, fill:TXT});
  I(c1, {type:"text", content:r[1] + " · account: " + r[2], fontSize:12, fill:MUT});
  const c2 = I(card, {type:"frame", name:"Transport", layout:"vertical", width:260, gap:2});
  I(c2, {type:"text", content:"Transport", fontSize:12, fontWeight:600, fill:MUT});
  I(c2, {type:"text", content:r[3], fontSize:13, fill:OK});
  const c3 = I(card, {type:"frame", name:"Auth", layout:"vertical", width:260, gap:2});
  I(c3, {type:"text", content:"Metadata auth", fontSize:12, fontWeight:600, fill:MUT});
  I(c3, {type:"text", content:r[4], fontSize:13, fill: r[4].startsWith("manual") ? WARN : OK});
  const c4 = I(card, {type:"frame", name:"Actions", layout:"vertical", width:220, gap:2});
  I(c4, {type:"text", content:"Supported: " + r[5], fontSize:12, fill:MUT});
  const ab = I(card, {type:"frame", name:"ActBtn", layout:"horizontal", width:170, height:32, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:PANEL2});
  I(ab, {type:"text", content:r[6], fontSize:13, fill:TXT});
}
const add = I(body, {type:"frame", name:"ConnectRow", layout:"horizontal", width:"fill_container", height:40, gap:12, alignItems:"center"});
I(add, {type:"text", content:"Second YouTube account and extra destinations supported: identity is (provider, channel), never platform alone.", fontSize:13, fill:MUT});
const cb = I(add, {type:"frame", name:"ConnectBtn", layout:"horizontal", width:200, height:32, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:ACCENT});
I(cb, {type:"text", content:"Connect account…", fontSize:13, fontWeight:600, fill:TXT});
