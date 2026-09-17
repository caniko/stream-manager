const BG = [{type:"solid",color:"#14161A"}];
const PANEL = [{type:"solid",color:"#1D2126"}];
const PANEL2 = [{type:"solid",color:"#232930"}];
const ACCENT = [{type:"solid",color:"#4C8DFF"}];
const ACCENT_BG = [{type:"solid",color:"#22344F"}];
const TXT = [{type:"solid",color:"#E8EAED"}];
const MUT = [{type:"solid",color:"#9AA3AD"}];
const OK = [{type:"solid",color:"#3FB950"}];
const WARN = [{type:"solid",color:"#E5A63B"}];
const LINE = [{type:"solid",color:"#2C333B"}];

const root = I(null, {type:"frame", name:"Streams 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
const tabNames = ["Streams","Destinations","Review","History","Video"];
for (const t of tabNames) {
  const on = t === "Streams";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:150, height:36, cornerRadius:8, gap:0, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
const banner = I(root, {type:"frame", name:"DemoBanner", layout:"horizontal", width:"fill_container", height:34, gap:8, padding:[6,16], alignItems:"center", fill:[{type:"solid",color:"#2A2410"}]});
I(banner, {type:"text", content:"DEMO — offline fixtures only. No credentials, no external calls.", fontSize:13, fontWeight:600, fill:[{type:"solid",color:"#E5A63B"}]});

const main = I(root, {type:"frame", name:"Main3Col", layout:"horizontal", width:"fill_container", height:814, gap:12, padding:[12,16]});
const list = I(main, {type:"frame", name:"StreamList", layout:"vertical", width:280, height:"fill_container", gap:8, padding:[12,12], cornerRadius:10, fill:PANEL});
I(list, {type:"text", content:"Streams", fontSize:15, fontWeight:700, fill:TXT});
const plans = [["Friday Build Stream","draft"],["Launch Week Q&A","ready"],["Archive: Spring Special","archived"]];
for (const p of plans) {
  const sel = p[0] === "Friday Build Stream";
  const card = I(list, {type:"frame", name:"Plan "+p[0], layout:"vertical", width:"fill_container", height:64, gap:4, padding:[8,10], cornerRadius:8, fill: sel ? ACCENT_BG : PANEL2});
  I(card, {type:"text", content:p[0], fontSize:14, fontWeight:600, fill:TXT});
  I(card, {type:"text", content:p[1] + " · rev 3 · saved 18:02", fontSize:12, fill:MUT});
}
const nb = I(list, {type:"frame", name:"NewDraftBtn", layout:"horizontal", width:"fill_container", height:38, cornerRadius:8, justifyContent:"center", alignItems:"center", fill:ACCENT});
I(nb, {type:"text", content:"+ New draft", fontSize:14, fontWeight:600, fill:TXT});

const ed = I(main, {type:"frame", name:"Editor", layout:"vertical", width:640, height:"fill_container", gap:10, padding:[14,16], cornerRadius:10, fill:PANEL});
I(ed, {type:"text", content:"Shared metadata", fontSize:15, fontWeight:700, fill:TXT});
const fields = [["Title","Friday Build Stream"],["Start (local)","2026-10-02 20:00"],["End (local, optional)","2026-10-02 22:00"],["Timezone","Europe/Berlin"],["Language","en"],["Category intent","Science & Technology"],["Tags","live, build, q&a"]];
for (const f of fields) {
  I(ed, {type:"text", content:f[0], fontSize:12, fontWeight:600, fill:MUT});
  const inp = I(ed, {type:"frame", name:"Field "+f[0], layout:"horizontal", width:"fill_container", height:36, gap:0, padding:[8,10], cornerRadius:8, alignItems:"center", fill:PANEL2});
  I(inp, {type:"text", content:f[1], fontSize:14, fill:TXT});
}
I(ed, {type:"text", content:"Description", fontSize:12, fontWeight:600, fill:MUT});
const ta = I(ed, {type:"frame", name:"DescArea", layout:"vertical", width:"fill_container", height:110, gap:0, padding:[8,10], cornerRadius:8, fill:PANEL2});
I(ta, {type:"text", content:"Live build session with Q&A. Bring questions.", fontSize:14, fill:TXT});
const stat = I(ed, {type:"frame", name:"StatusRow", layout:"horizontal", width:"fill_container", height:28, gap:12, alignItems:"center"});
I(stat, {type:"text", content:"● Autosaved 18:02:11 · rev 3", fontSize:12, fill:OK});
I(stat, {type:"text", content:"Archive · Reopen · Duplicate", fontSize:12, fill:MUT});

const prev = I(main, {type:"frame", name:"DestPreview", layout:"vertical", width:"fill_container", height:"fill_container", gap:8, padding:[12,12], cornerRadius:10, fill:PANEL});
I(prev, {type:"text", content:"Destinations · primary: YouTube", fontSize:15, fontWeight:700, fill:TXT});
const dests = [
  ["youtube-main · YouTube","PRIMARY","Full description (normal)","ok"],
  ["rumble-main · Rumble","FUNNEL → youtube-main","Override: preview stream…","warn"],
  ["kick-main · Kick","MIRROR","Inherit","ok"],
  ["twitch-main · Twitch","MIRROR","Inherit · no off-platform ask","ok"]
];
for (const d of dests) {
  const card = I(prev, {type:"frame", name:"Dest "+d[0], layout:"vertical", width:"fill_container", height:96, gap:4, padding:[8,10], cornerRadius:8, fill:PANEL2});
  I(card, {type:"text", content:d[0], fontSize:13, fontWeight:600, fill:TXT});
  I(card, {type:"text", content:d[1] + "  ·  role / override / funnel-target controls", fontSize:12, fill:MUT});
  I(card, {type:"text", content:d[2], fontSize:12, fill: d[3]==="warn" ? WARN : MUT});
}
