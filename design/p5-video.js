const BG = [{type:"solid",color:"#14161A"}];
const PANEL = [{type:"solid",color:"#1D2126"}];
const PANEL2 = [{type:"solid",color:"#232930"}];
const ACCENT = [{type:"solid",color:"#4C8DFF"}];
const ACCENT_BG = [{type:"solid",color:"#22344F"}];
const TXT = [{type:"solid",color:"#E8EAED"}];
const MUT = [{type:"solid",color:"#9AA3AD"}];
const OK = [{type:"solid",color:"#3FB950"}];
const WARN = [{type:"solid",color:"#E5A63B"}];

const root = I(null, {type:"frame", name:"Video 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
for (const t of ["Streams","Destinations","Review","History","Video"]) {
  const on = t === "Video";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:150, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
const note = I(root, {type:"frame", name:"Note", layout:"horizontal", width:"fill_container", height:36, gap:8, padding:[6,16], alignItems:"center", fill:[{type:"solid",color:"#2A2410"}]});
I(note, {type:"text", content:"PREVIEW ONLY — outgoing video is NOT transformed. Opaque masks are not guaranteed redaction.", fontSize:13, fontWeight:600, fill:WARN});
const body = I(root, {type:"frame", name:"Body", layout:"horizontal", width:"fill_container", height:"fill_container", gap:12, padding:[12,16]});
const left = I(body, {type:"frame", name:"ProfileList", layout:"vertical", width:330, height:"fill_container", gap:8, padding:[12,12], cornerRadius:10, fill:PANEL});
I(left, {type:"text", content:"Stored profiles (v1)", fontSize:15, fontWeight:700, fill:TXT});
for (const p of ["Passthrough (clean)","CTA banner + QR","Opaque region mask","Full-frame slate"]) {
  const sel = p === "CTA banner + QR";
  const c = I(left, {type:"frame", name:"P "+p, layout:"vertical", width:"fill_container", height:58, gap:2, padding:[8,10], cornerRadius:8, fill: sel ? ACCENT_BG : PANEL2});
  I(c, {type:"text", content:p, fontSize:13, fontWeight:600, fill:TXT});
  I(c, {type:"text", content:"assigned: rumble-main", fontSize:12, fill:MUT});
}
const mid = I(body, {type:"frame", name:"Preview", layout:"vertical", width:"fill_container", height:"fill_container", gap:8, padding:[12,12], cornerRadius:10, fill:PANEL});
I(mid, {type:"text", content:"Native preview — 16:9 frame, CTA banner + QR (bottom, 12%, 95%)", fontSize:14, fontWeight:600, fill:TXT});
const frame = I(mid, {type:"frame", name:"Frame169", layout:"vertical", width:"fill_container", height:420, gap:0, cornerRadius:10, fill:[{type:"solid",color:"#0B0D10"}]});
I(frame, {type:"text", content:"synthetic program picture", fontSize:12, fill:MUT});
const cta = I(frame, {type:"frame", name:"CTABanner", layout:"horizontal", width:"fill_container", height:64, gap:10, padding:[8,12], alignItems:"center", fill:[{type:"solid",color:"#0F2A52"}]});
I(cta, {type:"text", content:"Watch the full show live", fontSize:14, fontWeight:700, fill:TXT});
const qr = I(cta, {type:"frame", name:"QR", layout:"horizontal", width:48, height:48, cornerRadius:6, justifyContent:"center", alignItems:"center", fill:TXT});
I(qr, {type:"text", content:"QR", fontSize:12, fontWeight:700, fill:[{type:"solid",color:"#0B0D10"}]});
I(mid, {type:"text", content:"Geometry: placement bottom · size 0.12 · opacity 0.95 · safe margins pass · audio: keep · timing: whole program · link from reviewed rev 4", fontSize:12, fill:MUT});
const right = I(body, {type:"frame", name:"Geometry", layout:"vertical", width:300, height:"fill_container", gap:8, padding:[12,12], cornerRadius:10, fill:PANEL});
I(right, {type:"text", content:"Edit geometry", fontSize:15, fontWeight:700, fill:TXT});
for (const g of [["X","0.05"],["Y","0.70"],["W","0.90"],["H","0.22"],["Opacity","1.00"],["QR link","rev-4 URL"]]) {
  I(right, {type:"text", content:g[0], fontSize:12, fontWeight:600, fill:MUT});
  const inp = I(right, {type:"frame", name:"G "+g[0], layout:"horizontal", width:"fill_container", height:34, padding:[6,10], cornerRadius:8, alignItems:"center", fill:PANEL2});
  I(inp, {type:"text", content:g[1], fontSize:13, fill:TXT});
}
I(right, {type:"text", content:"Policy on Twitch output: no funnel overlay, no degrading mask.", fontSize:12, fill:OK});
