'use strict';
/** Run explicitly from the native desktop terminal. This is not a scheduler. */
const fs=require('node:fs');
const path=require('node:path');
const {capture,configFromEnvironment}=require('./headed_fetch.cjs');
const {statIfPresent,unlinkIfPresent,freshLease,reapResponses}=require('./queue_files.cjs');
const queue=path.resolve(process.env.INSANE_BROWSER_QUEUE || path.join(__dirname,'.queue'));
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const MAX_REQUEST=32768;
const processIdentity=()=>({pid:process.pid,boot_id:fs.readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim(),start_ticks:fs.readFileSync('/proc/self/stat','utf8').split(') ').at(-1).split(' ')[19]});
const identity=processIdentity();
function writeJSON(file,value){const temp=file+'.tmp';fs.writeFileSync(temp,JSON.stringify(value),{mode:0o600});fs.renameSync(temp,file);}
async function main(){
  if(!process.env.DISPLAY && !process.env.WAYLAND_DISPLAY)throw new Error('A real desktop session is required; no headless fallback');
  fs.mkdirSync(queue,{recursive:true,mode:0o700});
  const metadata=fs.lstatSync(queue);
  if(!metadata.isDirectory() || metadata.isSymbolicLink() || (metadata.mode & 0o077) || metadata.uid!==process.getuid())throw new Error('Browser queue must be owned by this user and private (0700)');
  const config={...configFromEnvironment(),cookieJar:new Map()};
  const lock=path.join(queue,'service.lock');
  const lockHandle=fs.openSync(lock,'wx',0o600);fs.writeSync(lockHandle,JSON.stringify(identity));fs.closeSync(lockHandle);
  let stopping=false;
  process.on('SIGTERM',()=>{stopping=true;});process.on('SIGINT',()=>{stopping=true;});
  const status=()=>writeJSON(path.join(queue,'status.json'),{pid:process.pid,identity,ready:true,updatedAt:Date.now(),backend:'playwright_headed_chromium',playwrightVersion:require('playwright/package.json').version,headless:false,chromiumSandbox:true});
  status();const heartbeat=setInterval(status,1000);
  try {
    while(!stopping && !fs.existsSync(path.join(queue,'stop'))){
      reapResponses(queue);
      const files=fs.readdirSync(queue).filter(name=>/^[a-f0-9]{32}\.request\.json$/.test(name)).sort();
      for(const name of files){
        if(stopping || fs.existsSync(path.join(queue,'stop')))break;
        const requestPath=path.join(queue,name),id=name.split('.')[0];
        const claimed=path.join(queue,id+'.working.json');
        try{fs.renameSync(requestPath,claimed);}catch(_){continue;}
        let response,cancellationError;
        try {
          if(!fs.lstatSync(claimed).isFile() || fs.lstatSync(claimed).isSymbolicLink() || fs.statSync(claimed).size>MAX_REQUEST)throw new Error('invalid_request_file');
          const request=JSON.parse(fs.readFileSync(claimed,'utf8'));
          if(request.id!==id || !Number.isFinite(request.expiresAt) || request.expiresAt<=Date.now())throw new Error('request_expired');
          const maxTime=Math.min(request.expiresAt-Date.now(),180000);
          const args={...request.args,timeout:Math.min(Number(request.args?.timeout)||90000,maxTime)};
          const lease=path.join(queue,id+'.lease');
          const cancelled=()=>{
            try{return stopping || fs.existsSync(path.join(queue,'stop')) || Date.now()>=request.expiresAt || !freshLease(lease);}
            catch(error){cancellationError=error;return true;}
          };
          if(cancelled())throw new Error('request_cancelled_or_expired');
          response={id,ok:true,envelope:await capture(args,{...config,cancelled})};
        } catch(e){response={id,ok:false,error:String(cancellationError?.message||e.message||e).slice(0,3000)};}
        const leasePath=path.join(queue,id+'.lease');
        if(freshLease(leasePath))
          writeJSON(path.join(queue,id+'.response.json'),response);
        unlinkIfPresent(claimed);
      }
      await sleep(100);
    }
  } finally {
    unlinkIfPresent(lock);clearInterval(heartbeat);writeJSON(path.join(queue,'status.json'),{pid:process.pid,identity,ready:false,updatedAt:Date.now()});
  }
}
main().catch(e=>{process.stderr.write(e.stack+'\n');process.exitCode=1;});
