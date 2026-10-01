'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {validateArgs,launchOptions,privateIP,assertPublicURL,boundaryFromState,capture}=require('./headed_fetch.cjs');
test('launch is always headed and sandboxed',()=>{const options=launchOptions();assert.equal(options.headless,false);assert.equal(options.chromiumSandbox,true);assert.equal(options.executablePath,'/usr/bin/chromium');assert.equal(options.ignoreHTTPSErrors,undefined);assert.equal(options.args,undefined);});
test('reject arbitrary launch options, profiles and secrets',()=>{for(const key of ['headless','args','ignoreHTTPSErrors','chromiumSandbox','profileDir','cookies','headers','proxy','executablePath','storageState'])assert.throws(()=>validateArgs({url:'https://example.com', [key]:true}),/forbidden/);});
test('URL credentials and unsafe schemes rejected',()=>{for(const url of ['file:///etc/passwd','javascript:alert(1)','http://name:password@example.com'])assert.throws(()=>validateArgs({url}));});
test('budget and selector bounds enforced',()=>{assert.equal(validateArgs({url:'https://example.com',timeout:999999}).timeout,180000);assert.throws(()=>validateArgs({url:'https://example.com',waitSelector:'x'.repeat(2049)}));});
test('private, metadata and alternate-IP forms blocked',async()=>{for(const value of ['127.0.0.1','10.1.1.1','169.254.169.254','192.168.1.1','::1','::ffff:127.0.0.1','::ffff:7f00:1','fe80::1','fc00::1'])assert.equal(privateIP(value),true,value);for(const url of ['http://127.1','http://2130706433','http://0x7f000001','http://localhost','http://[::1]'])await assert.rejects(assertPublicURL(url),/ssrf_blocked/);});
test('public DNS verified and mixed-address answer rejected',async()=>{await assertPublicURL('https://example.com',{lookup:async()=>[{address:'93.184.216.34'}]});await assert.rejects(assertPublicURL('https://example.com',{lookup:async()=>[{address:'93.184.216.34'},{address:'10.0.0.1'}]}),/ssrf_blocked/);await assert.rejects(assertPublicURL('https://example.com',{lookup:async()=>{throw Error('DNS failed');}}),/DNS failed/);});
test('fixture exception is exact-origin and process-owned',async()=>{await assertPublicURL('http://127.0.0.1:8080/fixture',{fixtureOrigin:'http://127.0.0.1:8080'});await assert.rejects(assertPublicURL('http://127.0.0.1:8081/fixture',{fixtureOrigin:'http://127.0.0.1:8080'}));});
test('auth and paywalls produce terminal boundaries',()=>{assert.equal(boundaryFromState({body:'Sign in to continue'}),'authentication_required');assert.equal(boundaryFromState({body:'Subscribe to read the rest'}),'paywall');assert.equal(boundaryFromState({body:'Article',passwordVisible:true}),'authentication_required');assert.equal(boundaryFromState({body:'Article'},401),'authentication_required');assert.equal(boundaryFromState({body:'Public documentation. Login links are optional.'},200),null);});
test('no display cannot silently fall back to headless',async()=>{const d=process.env.DISPLAY,w=process.env.WAYLAND_DISPLAY;delete process.env.DISPLAY;delete process.env.WAYLAND_DISPLAY;try{await assert.rejects(capture({url:'https://example.com'}),/headed_display_unavailable/);}finally{if(d)process.env.DISPLAY=d;if(w)process.env.WAYLAND_DISPLAY=w;}});
test('HTTP 200 infrastructure stub is not successful public content',()=>{assert.equal(boundaryFromState({body:'Site Unavailable\n\nUnable to access this site.'},200),'access_unavailable');assert.equal(boundaryFromState({body:'This article quotes Site Unavailable in a discussion.'},200),null);});
const {statIfPresent,unlinkIfPresent,freshLease,reapResponses}=require('./queue_files.cjs');
const vanished=()=>{const e=new Error('consumed');e.code='ENOENT';throw e;};
test('response removed after directory enumeration is normal',()=>{
  const id='a'.repeat(32);
  const io={readdirSync:()=>[id+'.response.json'],statSync:vanished,unlinkSync:()=>{throw Error('no unlink should be needed');}};
  assert.doesNotThrow(()=>reapResponses('/queue',100000,io));
});
test('lease removed before stat and files removed before unlink are normal',()=>{
  const id='b'.repeat(32);const removed=[];
  const io={readdirSync:()=>[id+'.response.json'],statSync:file=>{if(file.endsWith('.lease'))return vanished();return {mtimeMs:1};},unlinkSync:file=>{removed.push(file);return vanished();}};
  assert.equal(freshLease('/queue/'+id+'.lease',100000,io),false);
  assert.doesNotThrow(()=>reapResponses('/queue',100000,io));
  assert.equal(removed.length,2);
});
test('other queue filesystem errors stay visible',()=>{
  const error=Object.assign(new Error('denied'),{code:'EACCES'});
  const io={statSync:()=>{throw error;},unlinkSync:()=>{throw error;}};
  assert.throws(()=>statIfPresent('/queue/item',io),{code:'EACCES'});
  assert.throws(()=>unlinkIfPresent('/queue/item',io),{code:'EACCES'});
});
