#!/usr/bin/env python3
"""Explicit native-host lifetime launcher; no boot registration or readiness claim."""
import argparse,fcntl,json,os,stat,subprocess,sys,time
from pathlib import Path
import gateway_supervisor as sup
ROOT=Path(__file__).resolve().parent

def launch(runtime,proxy_config,offline_child=None):
    sup.private_dir(runtime)
    fd=os.open(runtime/'launcher.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    info=os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
        os.close(fd)
        raise ValueError('unsafe_launcher_lock')
    try:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=sup.inspect(runtime)
        if state['status'] not in {'not_started','inactive'}:
            return {'accepted':False,'reason':'existing_supervisor_or_orphan','state':state}
        env=os.environ.copy()
        if proxy_config:env.update(sup.load_proxy_config(proxy_config))
        # Only this launcher's structured logs: bound retained history to 3 files.
        for old,new in [('supervisor.log.1','supervisor.log.2'),('supervisor.log','supervisor.log.1')]:
            source=runtime/old
            if source.exists(): os.replace(source,runtime/new)
        logfd=os.open(runtime/'supervisor.log',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        args=[sys.executable,str(ROOT/'gateway_supervisor.py'),'run','--runtime',str(runtime)]
        if proxy_config:args+=['--proxy-config',str(proxy_config),'--expected-proxy-sha256',sup.proxy_fingerprint(env)]
        if offline_child:args+=['--offline-child',str(offline_child)]
        try:
            proc=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=logfd,stderr=subprocess.DEVNULL,
                close_fds=True,start_new_session=True,env=env)
        finally:os.close(logfd)
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            state=sup.inspect(runtime)
            if state.get('identity',{}).get('pid')==proc.pid and state['status'] in {'running','latched','stopped','backoff'}:
                return {'accepted':state['status'] in {'running','backoff'},'supervisor_pid':proc.pid,'state':state,'discord_readiness':'not_claimed'}
            if proc.poll() is not None:
                return {'accepted':False,'reason':'supervisor_exited','returncode':proc.returncode,'state':state}
            time.sleep(.1)
        return {'accepted':False,'reason':'startup_unconfirmed','supervisor_pid':proc.pid,'state':state}
    finally:os.close(fd)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['launch','status','stop'])
    p.add_argument('--runtime',type=Path,default=ROOT/'.gateway-supervisor')
    p.add_argument('--proxy-config',type=Path,default=ROOT/'gateway_native_proxy_config.json')
    p.add_argument('--offline-child',type=Path)
    p.add_argument('--result-file',type=Path)
    a=p.parse_args(); runtime=a.runtime.absolute()
    if a.action=='launch':result=launch(runtime,a.proxy_config.absolute(),a.offline_child.absolute() if a.offline_child else None)
    elif a.action=='status':result=sup.inspect(runtime)
    else:result={'stop_returncode':sup.validated_stop(runtime)}
    encoded=json.dumps(result,sort_keys=True)
    if a.result_file:
        fd=os.open(a.result_file,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
            os.close(fd)
            raise ValueError('unsafe_result_file')
        with os.fdopen(fd,'w') as f:
            f.truncate(0)
            f.write(encoded)
    print(encoded)
if __name__=='__main__':
    try:main()
    except (OSError,ValueError):
        print('{"accepted":false,"reason":"local_config_or_io_error"}')
        raise SystemExit(1)
