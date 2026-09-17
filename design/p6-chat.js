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

const root = I(null, {type:"frame", name:"Chat 1440", width:1440, height:900, layout:"vertical", gap:0, fill:BG, padding:[0,0]});
const tabs = I(root, {type:"frame", name:"TabBar", layout:"horizontal", width:"fill_container", height:52, gap:8, padding:[8,16], fill:PANEL});
for (const t of ["Streams","Destinations","Review","History","Video","Chat"]) {
  const on = t === "Chat";
  const b = I(tabs, {type:"frame", name:"Tab "+t, layout:"horizontal", width:140, height:36, cornerRadius:8, justifyContent:"center", alignItems:"center", fill: on ? ACCENT_BG : PANEL2});
  I(b, {type:"text", content:t, fontSize:14, fontWeight: on ? 700 : 400, fill: on ? TXT : MUT});
}
I(root, {type:"text", content:"Private operator console — viewer messages stay here. Never auto-forward between platforms.", fontSize:12, fill:MUT});
const body = I(root, {type:"frame", name:"Body", layout:"horizontal", width:"fill_container", height:"fill_container", gap:10, padding:[12,16]});
const left = I(body, {type:"frame", name:"Sources", layout:"vertical", width:270, gap:8, padding:[10,10], cornerRadius:10, fill:PANEL});
I(left, {type:"text", content:"Sources", fontSize:16, fontWeight:700, fill:TXT});
for (const s of [["[TW] alice","live · R/W"],["[YT] alice","rate-limited · R/W"],["[KK] alice","relay required"],["[RU] alice","live · read-only"]]) {
  const c = I(left, {type:"frame", name:"Src", layout:"vertical", width:"fill_container", gap:2, padding:[8,8], cornerRadius:8, fill:PANEL2});
  I(c, {type:"text", content:s[0], fontSize:14, fontWeight:600, fill:TXT});
  I(c, {type:"text", content:s[1], fontSize:12, fill: s[1].startsWith("live") ? OK : WARN});
}
I(left, {type:"text", content:"Proposed from plan: kick-main (Connect) · Accounts: alice connected as alice (+send)", fontSize:12, fill:MUT});
const center = I(body, {type:"frame", name:"Feed", layout:"vertical", width:"fill_container", gap:8, padding:[10,12], cornerRadius:10, fill:PANEL});
I(center, {type:"text", content:"[x] YouTube [x] Twitch [x] Kick [x] Rumble [ ] mentions [ ] paid · search · follow [x] · A A+ A++", fontSize:12, fill:MUT});
for (const m of [
  ["[TW] bob · 10:01","hello from twitch","",TXT],
  ["[KK] bob · 10:01","hello from kick (same name, other platform)","",TXT],
  ["[YT] patron · 10:02","take my $5 [paid]","",WARN],
  ["… possible gap on twitch: reconnect window elapsed; events during the outage are not replayed","","",MUT],
  ["[RU] amy · 10:03","gg","",TXT],
  ["(deleted on twitch: tw-2)","","",MUT]
]) {
  I(center, {type:"text", content:m[0], fontSize:12, fill:MUT});
  if (m[1]) I(center, {type:"text", content:m[1], fontSize:15, fill:m[3]});
}
I(center, {type:"text", content:"To twitch:alice as alice · Replying to bob (tw only) [clear] · [message…              ] [Send]", fontSize:13, fill:TXT});
const right = I(body, {type:"frame", name:"Details", layout:"vertical", width:300, gap:6, padding:[10,10], cornerRadius:10, fill:PANEL});
I(right, {type:"text", content:"Details", fontSize:16, fontWeight:700, fill:TXT});
I(right, {type:"text", content:"Source: twitch:alice · Remote id: m-1 · Author id: 222 · Local seq: #4 · Badges: subscriber · Moderation: Visible", fontSize:12, fill:MUT});
I(right, {type:"text", content:"Outbox: chat-1-9 confirmed · chat-1-12 outcome unknown (not retried)", fontSize:12, fill:MUT});
