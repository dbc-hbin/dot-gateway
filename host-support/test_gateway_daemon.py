"""Offline launcher checks; fake children only, no live ledger/network/token."""
import json,os,subprocess,sys,tempfile,time,unittest
from pathlib import Path
import gateway_daemon as d
import gateway_supervisor as s

class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.base=Path(self.temp.name);self.runtime=self.base/'run'
        self.child=self.base/'child.py';self.child.write_text('import time\ntry: time.sleep(60)\nexcept KeyboardInterrupt: pass\n')
        self.proxy=self.base/'proxy.json';self.proxy.write_text(json.dumps({'BRIDGE_HTTPS_PROXY':'http://localhost:99','BRIDGE_NO_PROXY':''}));self.proxy.chmod(0o600)
    def tearDown(self):
        if self.runtime.exists():
            if s.inspect(self.runtime).get('identity'):s.validated_stop(self.runtime)
            until=time.monotonic()+3
            while s.inspect(self.runtime).get('identity') and time.monotonic()<until:time.sleep(.02)
        self.temp.cleanup()
    def test_launch_duplicate_and_graceful_stop(self):
        a=d.launch(self.runtime,self.proxy,self.child);self.assertTrue(a['accepted'])
        self.assertFalse(d.launch(self.runtime,self.proxy,self.child)['accepted'])
        self.assertEqual(s.validated_stop(self.runtime),0)
        until=time.monotonic()+3
        while s.inspect(self.runtime).get('identity') and time.monotonic()<until:time.sleep(.02)
        self.assertEqual(s.inspect(self.runtime)['last_status'],'stopped')
    def test_bad_proxy_launches_nothing(self):
        self.proxy.write_text('{}')
        with self.assertRaises(ValueError):d.launch(self.runtime,self.proxy,self.child)
        self.assertFalse((self.runtime/'supervisor.log').exists())
    def test_fifo_config_and_lock_rejected(self):
        fifo=self.base/'fifo';os.mkfifo(fifo,0o600)
        with self.assertRaises(ValueError):s.load_proxy_config(fifo)
        s.private_dir(self.runtime);os.mkfifo(self.runtime/'launcher.lock',0o600)
        with self.assertRaises(ValueError):d.launch(self.runtime,self.proxy,self.child)
    def test_unsafe_result_file_not_truncated(self):
        out=self.base/'result';out.write_text('preserve');out.chmod(0o644)
        p=subprocess.run([sys.executable,str(Path(d.__file__)),'status','--runtime',str(self.runtime),'--result-file',str(out)],capture_output=True)
        self.assertEqual(p.returncode,1);self.assertEqual(out.read_text(),'preserve')
if __name__=='__main__':unittest.main(verbosity=2)
