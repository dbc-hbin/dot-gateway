'use strict';
// Queue files are consumed concurrently by the native helper and Python client.
// Disappearance is normal; permission, disk, and other I/O failures are not.
const fs=require('node:fs'),path=require('node:path');
function statIfPresent(file,io=fs){
  try{return io.statSync(file);}catch(error){if(error.code==='ENOENT')return null;throw error;}
}
function unlinkIfPresent(file,io=fs){
  try{io.unlinkSync(file);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}
}
function freshLease(file,now=Date.now(),io=fs){
  const metadata=statIfPresent(file,io);
  return !!metadata && now-metadata.mtimeMs<=10000;
}
function reapResponses(queue,now=Date.now(),io=fs){
  for(const name of io.readdirSync(queue).filter(value=>/^[a-f0-9]{32}\.response\.json$/.test(value))){
    const response=path.join(queue,name),lease=path.join(queue,name.split('.')[0]+'.lease');
    const metadata=statIfPresent(response,io);
    if(!metadata || now-metadata.mtimeMs<=30000)continue;
    if(!freshLease(lease,now,io)){
      unlinkIfPresent(response,io);
      unlinkIfPresent(lease,io);
    }
  }
}
module.exports={statIfPresent,unlinkIfPresent,freshLease,reapResponses};
