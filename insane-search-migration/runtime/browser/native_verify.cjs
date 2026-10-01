'use strict';
const fs=require('node:fs'),path=require('node:path'),http=require('node:http'),assert=require('node:assert/strict');
const {capture,configFromEnvironment}=require('./headed_fetch.cjs');
(async()=>{
  const results=[];let server, blockedServer;let blockedHits=0,webSocketHits=0;
  const report=()=>fs.writeFileSync(path.join(__dirname,'native-verification.json'),JSON.stringify({playwright:require('playwright/package.json').version,headless:false,chromiumSandbox:true,results},null,2));
  try {
    blockedServer=http.createServer((req,res)=>{blockedHits++;res.end('must not be reached');});
    blockedServer.on('upgrade',(req,socket)=>{webSocketHits++;socket.destroy();});
    await new Promise(r=>blockedServer.listen(0,'127.0.0.1',r));
    const blockedOrigin=`http://127.0.0.1:${blockedServer.address().port}`;
    server=http.createServer((req,res)=>{
      res.setHeader('Content-Type','text/html');
      if(req.url==='/redirect'){res.writeHead(302,{Location:blockedOrigin+'/blocked'});res.end();return;}
      if(req.url==='/redirect-public'){res.writeHead(302,{Location:'/redirect'});res.end();return;}
      if(req.url==='/websocket'){res.end('<title>Socket fixture</title><h1>Public socket test</h1><script>new WebSocket("'+blockedOrigin.replace('http:','ws:')+'/socket")</script>');return;}
      if(req.url==='/hang'){res.end('<title>Timeout fixture</title><script>setTimeout(()=>{while(true){}},100)</script>');return;}
      if(req.url==='/cookie-check'){res.end('<title>Cookie fixture</title><article>'+String(req.headers.cookie||'none')+'</article>');return;}
      if(req.url==='/login'){res.end('<title>Login fixture</title><h1>Sign in to continue</h1><input type="password">');return;}
      if(req.url==='/paywall'){res.end('<title>Paywall fixture</title><h1>Subscribe to read the rest</h1>');return;}
      res.setHeader('Set-Cookie','public_fixture=value; Path=/; SameSite=Lax');
      res.end('<!doctype html><title>insane-search headed adapter verification</title><h1>Visible sandboxed Linux browser</h1><article id="content">'+('Public fixture content. '.repeat(200))+'</article><script>document.querySelector("h1").textContent="Rendered JS verified: Playwright headed"</script>');
    });
    await new Promise(r=>server.listen(0,'127.0.0.1',r));
    const origin=`http://127.0.0.1:${server.address().port}`;
    const config={fixtureOrigin:origin,cookieJar:new Map()};
    const good=await capture({url:origin+'/',waitSelector:'#content',timeout:45000},{...config,screenshotPath:path.join(__dirname,'native-adapter-page.png'),holdMs:Number(process.env.INSANE_VERIFY_HOLD_MS ?? 20000)});
    assert.equal(good.status,200);assert.match(good.innerText,/Rendered JS verified/);assert.ok(good.cookies.some(c=>c.name==='public_fixture'));assert.equal(good.terminalReason,null);
    results.push({case:'rendered_local_fixture',ok:true,status:good.status,browserVersion:good.browserVersion,cookieBridgeValidated:true});report();
    const reuse=await capture({url:origin+'/cookie-check',timeout:15000},config);
    assert.match(reuse.innerText,/public_fixture=value/);
    results.push({case:'public_cookie_reuse',ok:true});report();
    const hungStarted=Date.now();
    await assert.rejects(capture({url:origin+'/hang',timeout:2500},config));
    assert.ok(Date.now()-hungStarted<10000);
    const recovered=await capture({url:origin+'/',timeout:15000},config);
    assert.equal(recovered.status,200);
    results.push({case:'timeout_then_next_request_recovery',ok:true,elapsedMs:Date.now()-hungStarted});report();
    for(const kind of ['login','paywall']){
      const value=await capture({url:origin+'/'+kind,timeout:15000},config);
      assert.equal(value.terminalReason,kind==='login'?'authentication_required':'paywall');assert.deepEqual(value.cookies,[]);
      results.push({case:kind,ok:true,status:value.status,terminalReason:value.terminalReason});report();
    }
    await assert.rejects(capture({url:origin+'/redirect',timeout:15000},config),/ERR_BLOCKED_BY_CLIENT|ssrf_blocked/);
    await assert.rejects(capture({url:origin+'/redirect-public',timeout:15000},config),/ERR_BLOCKED_BY_CLIENT|ssrf_blocked/);
    assert.equal(blockedHits,0);
    results.push({case:'private_redirect_blocked',ok:true,blockedHits});report();
    await capture({url:origin+'/websocket',timeout:15000},config);
    assert.equal(webSocketHits,0);results.push({case:'private_websocket_blocked',ok:true,webSocketHits});report();
    const publicResult=await capture({url:'https://example.com/',waitSelector:'h1',timeout:30000},configFromEnvironment());
    assert.equal(publicResult.status,200);assert.match(publicResult.innerText,/documentation examples/);
    results.push({case:'public_example_documentation',ok:true,status:publicResult.status,finalUrl:publicResult.finalUrl});report();
  }catch(e){results.push({case:'failure',ok:false,error:e.stack});report();process.exitCode=1;}
  finally {if(server)await new Promise(r=>server.close(r));if(blockedServer)await new Promise(r=>blockedServer.close(r));}
})();
