'use strict';
/** Public-only, real-display Playwright capture. No stealth forks or CDP attach. */
const dns = require('node:dns').promises;
const net = require('node:net');
const fs = require('node:fs');
const {chromium, devices} = require('playwright');
const MAX_HTML = 8 * 1024 * 1024;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const CHALLENGE_MARKERS = ['Just a moment...', 'window._cf_chl_opt', 'cf-chl-bypass',
  'verify you are human', "verify you're human", 'checking your browser', 'press and hold',
  'human verification', 'sec-if-cpt-container'];
function privateIP(value) {
  let ip = value.replace(/^\[|\]$/g, '').toLowerCase();
  if (ip.startsWith('::ffff:')) {
    const tail = ip.slice(7);
    if (tail.includes('.')) return privateIP(tail);
    const parts = tail.split(':');
    if (parts.length === 2) {
      const a = parseInt(parts[0],16), b = parseInt(parts[1],16);
      return privateIP(`${a>>8}.${a&255}.${b>>8}.${b&255}`);
    }
  }
  if (net.isIP(ip) === 4) {
    const [a,b,c] = ip.split('.').map(Number);
    return a===0 || a===10 || a===127 || a>=224 ||
      (a===169 && b===254) || (a===172 && b>=16 && b<=31) ||
      (a===192 && (b===168 || b===0 || (b===88 && c===99))) ||
      (a===100 && b>=64 && b<=127) || (a===198 && (b===18 || b===19)) ||
      (a===198 && b===51 && c===100) || (a===203 && b===0 && c===113);
  }
  if (net.isIP(ip) === 6) return !(ip.startsWith('2') || ip.startsWith('3')) || ip.startsWith('2001:db8:');
  return true;
}
function parsedURL(value) {
  const url = new URL(value);
  if (!['http:','https:'].includes(url.protocol)) throw new Error('ssrf_blocked: non-http(s) URL');
  if (url.username || url.password) throw new Error('credentials_in_url_forbidden');
  return url;
}
async function assertPublicURL(value, config = {}) {
  const url = parsedURL(value);
  // Test permission is process-owned and restricted to one exact origin. It is
  // never read from an untrusted queue request or from browser content.
  if (config.fixtureOrigin && url.origin === config.fixtureOrigin) return url;
  const hostname = url.hostname.replace(/^\[|\]$/g,'');
  if (hostname === 'localhost' || /\.(localhost|local|internal)$/i.test(hostname)) throw new Error('ssrf_blocked: private hostname');
  if (net.isIP(hostname)) {
    if (privateIP(hostname)) throw new Error('ssrf_blocked: private IP');
    return url;
  }
  const addresses = await Promise.race([
    (config.lookup || dns.lookup)(hostname, {all:true}),
    new Promise((_,reject)=>{const timer=setTimeout(()=>reject(new Error('DNS timeout')),4000); timer.unref();})
  ]);
  if (!addresses.length || addresses.some(({address})=>privateIP(address))) throw new Error('ssrf_blocked: DNS did not resolve exclusively public addresses');
  return url;
}
function validateArgs(args) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('JSON object required');
  if (typeof args.url !== 'string' || args.url.length > 8192) throw new Error('valid bounded URL required');
  parsedURL(args.url);
  for (const name of ['headless','ignoreHTTPSErrors','chromiumSandbox','args','executablePath','profileDir','storageState','proxy','cookies','headers']) {
    if (name in args) throw new Error(`caller browser option forbidden: ${name}`);
  }
  if (args.waitSelector && (typeof args.waitSelector !== 'string' || args.waitSelector.length>2048)) throw new Error('invalid selector');
  return {...args, timeout:Math.max(1000,Math.min(Number(args.timeout)||90000,180000)),
    settleMs:Math.max(0,Math.min(Number(args.settleMs) || 2500,5000))};
}
function launchOptions(config={}) {
  const options = {headless:false, chromiumSandbox:true, executablePath:config.executablePath || '/usr/bin/chromium'};
  if (config.proxy) options.proxy = config.proxy;
  return options;
}
function boundaryFromState(state,status=0) {
  if (status === 401 || status === 407 || state.passwordVisible) return 'authentication_required';
  const text = String(state.body||'');
  if (/^Site Unavailable\s+Unable to access this site\.?$/i.test(text.trim())) return 'access_unavailable';
  if (/subscribe (?:to read|to continue)|subscription (?:is )?required|members[- ]only content|already a subscriber\?\s*(?:sign|log) in/i.test(text)) return 'paywall';
  if (/(?:sign|log) in (?:to continue|to view|to read|to access)|login required|authentication required/i.test(text)) return 'authentication_required';
  return null;
}
async function readState(page) {
  return page.evaluate(()=>{
    const html=document.documentElement?.outerHTML || '';
    return {html:html.slice(0,8*1024*1024),htmlTooLarge:html.length>8*1024*1024,
    title:(document.title || '').slice(0,4096),body:(document.body?.innerText || '').slice(0,1000000),
    passwordVisible:[...document.querySelectorAll('input[type="password"]')].some(e=>!!(e.offsetWidth||e.offsetHeight||e.getClientRects().length)),
    userAgent:navigator.userAgent
  };});
}
async function capture(input, config={}) {
  const args=validateArgs(input);
  if (!config.display && !process.env.DISPLAY && !process.env.WAYLAND_DISPLAY) throw new Error('headed_display_unavailable: start the helper in the real desktop terminal');
  await assertPublicURL(args.url,config);
  const started=Date.now(); const deadline=started+args.timeout;
  const origin=new URL(args.url).origin;
  const jarKey=origin+'|'+(args.device || 'desktop');
  const remaining=()=>Math.max(1,deadline-Date.now());
  let browser, context, page, status=0, aborted=false; const observations=[];
  const abort=()=>{aborted=true; if(context)context.close().catch(()=>{}); if(browser)browser.close().catch(()=>{});};
  const deadlineTimer=setTimeout(abort,args.timeout);
  const cancellationTimer=config.cancelled ? setInterval(()=>{if(config.cancelled())abort();},500) : null;
  try {
    browser=await (config.chromium || chromium).launch({...launchOptions(config),timeout:remaining()});
    if(aborted)throw new Error('capture_cancelled_or_expired');
    const device=args.device === 'mobile' ? devices['iPhone 13 Pro'] : {};
    context=await browser.newContext({...device,viewport:device.viewport || {width:1280,height:800}, serviceWorkers:'block',acceptDownloads:false});
    if(config.cookieJar) {
      const previous=config.cookieJar.get(jarKey);
      if(previous && Date.now()-previous.savedAt<900000)await context.addCookies(previous.cookies);
    }
    // Request routing does not cover WebSockets; this public capture does not
    // require them, so deny all sockets before navigation.
    await context.routeWebSocket('**/*',socket=>socket.close());
    context.setDefaultTimeout(Math.min(10000,args.timeout));
    await context.route('**/*',async route=>{
      try {
        if(route.request().isNavigationRequest() && page && route.request().frame()!==page.mainFrame())throw new Error('child_document_blocked');
        await assertPublicURL(route.request().url(),config); await route.continue();
      }
      catch(e){if(observations.length<128)observations.push({type:'blocked_request',reason:String(e.message).slice(0,200)}); await route.abort('blockedbyclient');}
    });
    // Browser-created popups have no role in a public page capture.
    context.on('page',extra=>{if(page && extra!==page)extra.close().catch(()=>{});});
    page=await context.newPage();
    // Playwright route() covers only the initial URL in a redirect chain.
    // Pause redirect *responses* in our own freshly-launched browser before
    // Chromium can follow their Location. This is not a CDP endpoint attach.
    const cdp=await context.newCDPSession(page);
    cdp.on('Fetch.requestPaused',async event=>{
      try {
        await assertPublicURL(event.request.url,config);
        if(event.responseStatusCode>=300 && event.responseStatusCode<400) {
          const location=(event.responseHeaders||[]).find(h=>h.name.toLowerCase()==='location');
          if(location)await assertPublicURL(new URL(location.value,event.request.url).href,config);
        }
        await cdp.send('Fetch.continueResponse',{requestId:event.requestId});
      }catch(e){
        if(observations.length<128)observations.push({type:'blocked_redirect',reason:String(e.message).slice(0,200)});
        await cdp.send('Fetch.failRequest',{requestId:event.requestId,errorReason:'BlockedByClient'}).catch(()=>{});
      }
    });
    await cdp.send('Fetch.enable',{patterns:[{urlPattern:'*',requestStage:'Response'}]});
    page.on('dialog',dialog=>dialog.dismiss().catch(()=>{}));
    page.on('response',response=>{if(response.request().isNavigationRequest() && response.frame()===page.mainFrame())status=response.status();});
    await page.goto(args.url,{waitUntil:'domcontentloaded',timeout:remaining()});
    await sleep(Math.min(args.settleMs,remaining()));
    let state=await readState(page); let boundary=boundaryFromState(state,status);
    if (!boundary && args.waitSelector) {
      try {await page.waitForSelector(args.waitSelector,{state:'visible',timeout:Math.min(10000,remaining())});}
      catch(_){observations.push({type:'selector_missing'});}
      state=await readState(page); boundary=boundaryFromState(state,status);
    }
    const lower=(state.html+'\n'+state.body).toLowerCase();
    const markers=CHALLENGE_MARKERS.filter(m=>lower.includes(m.toLowerCase()));
    if (state.htmlTooLarge || Buffer.byteLength(state.html)>MAX_HTML) throw new Error('capture_too_large');
    await assertPublicURL(page.url(),config);
    // Screenshots are deliberately process-configured, never request paths.
    if(config.screenshotPath)await page.screenshot({path:config.screenshotPath});
    if(config.holdMs)await sleep(Math.min(config.holdMs,60000));
    const cookies=boundary ? [] : (await context.cookies()).filter(c=>{
      const host=new URL(page.url()).hostname; const domain=c.domain.replace(/^\./,'');
      return host===domain || host.endsWith('.'+domain);
    }).map(({name,value,domain,path,expires,httpOnly,secure,sameSite})=>({name,value,domain,path,expires,httpOnly,secure,sameSite}));
    if(config.cookieJar) {
      if(boundary)config.cookieJar.delete(jarKey);
      else if(new URL(page.url()).origin===origin)config.cookieJar.set(jarKey,{savedAt:Date.now(),cookies});
      while(config.cookieJar.size>64)config.cookieJar.delete(config.cookieJar.keys().next().value);
    }
    return {html:state.html,finalUrl:page.url(),status,cookies,userAgent:state.userAgent,
      automation:'playwright_headed_chromium',innerText:state.body.slice(0,1000000),
      challenge:{initialMarkers:markers,finalMarkers:markers,resolved:markers.length===0,waitedMs:Date.now()-started,domStable:false,captchaClicks:0},
      observations,terminalReason:boundary,headless:false,chromiumSandbox:true,browserVersion:browser.version()};
  } finally {clearTimeout(deadlineTimer);if(cancellationTimer)clearInterval(cancellationTimer);await Promise.race([Promise.all([context?.close().catch(()=>{}),browser?.close().catch(()=>{})]),sleep(5000)]);}
}
function configFromEnvironment() {
  const config={};
  if(process.env.INSANE_CHROMIUM_PATH)config.executablePath=process.env.INSANE_CHROMIUM_PATH;
  if(process.env.INSANE_BROWSER_PROXY_CONFIG){
    const settings=JSON.parse(fs.readFileSync(process.env.INSANE_BROWSER_PROXY_CONFIG,'utf8'));
    const proxy=new URL(settings.BRIDGE_HTTPS_PROXY);
    if(!['http:','https:'].includes(proxy.protocol)||proxy.username||proxy.password)throw new Error('Only credential-free configured HTTP(S) proxy supported');
    config.proxy={server:proxy.origin,bypass:settings.BRIDGE_NO_PROXY || 'localhost,127.0.0.1,[::1]'};
  }
  return config;
}
module.exports={capture,validateArgs,launchOptions,assertPublicURL,privateIP,boundaryFromState,configFromEnvironment};
if(require.main===module){
  let data='';process.stdin.setEncoding('utf8');
  process.stdin.on('data',chunk=>{data+=chunk;if(data.length>32768){process.stderr.write('request_too_large\n');process.exit(2);}});
  process.stdin.on('end',async()=>{try {const result=await capture(JSON.parse(data),configFromEnvironment());process.stdout.write(JSON.stringify(result));}catch(e){process.stderr.write(`${e.name}: ${e.message}\n`);process.exitCode=1;}});
}
