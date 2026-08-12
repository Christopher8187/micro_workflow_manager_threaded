#!/usr/bin/env python3
"""A/B central network-manager dispatch on the 22-node 20:1 skew benchmark."""
from __future__ import annotations
import argparse, json, statistics, tempfile, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode
from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import close_shared_http_transport, configure_shared_http_transport, shared_http_transport
from micro_workflow_manager.runners.api import ApiRunner

JOBS = [2000, 2000] + [100] * 20
NAMES = [f"H{i:02d}" for i in range(22)]

def weighted_split(total, weights):
    raw = [total * w / sum(weights) for w in weights]
    values = [max(1, int(x)) for x in raw]
    while sum(values) < total:
        i = max(range(len(values)), key=lambda j: (raw[j] - values[j], weights[j])); values[i] += 1
    while sum(values) > total:
        candidates = [i for i,x in enumerate(values) if x > 1]
        i = max(candidates, key=lambda j: (values[j] - raw[j], -weights[j])); values[i] -= 1
    return values

class Recorder:
    def __init__(self):
        self.lock=threading.Lock(); self.data={name:{"first":None,"last":None,"send":[],"recv":[]} for name in NAMES}
    def start(self,name):
        now=time.perf_counter()
        with self.lock:
            if self.data[name]["first"] is None: self.data[name]["first"]=now
    def finish(self,name,response):
        perf=time.perf_counter(); mono=time.monotonic(); ext=response.extensions.get("mwf_network_manager",{})
        with self.lock:
            row=self.data[name]; row["last"]=perf
            if isinstance(ext.get("submitted_at"),(int,float)) and isinstance(ext.get("dispatched_at"),(int,float)):
                row["send"].append(max(0.0, ext["dispatched_at"]-ext["submitted_at"]))
            if isinstance(ext.get("completed_at"),(int,float)):
                row["recv"].append(max(0.0, mono-ext["completed_at"]))

def pct(values,q):
    if not values:return 0.0
    values=sorted(values); return values[min(len(values)-1,round((len(values)-1)*q))]

def url(args):
    return args.endpoint.rstrip('/')+'/transfer?'+urlencode({"bytes":args.response_bytes,"bps":args.bytes_per_second,"delay_ms":args.delay_ms,"chunk":4096})

def configure(args):
    close_shared_http_transport(); configure_shared_http_transport(http2=args.http2,streams_per_connection=80,
        architecture=args.architecture,state_flush_interval=2.0,verify=False if args.http2 else True)

def handler(name,target,args,rec):
    def call(ctx, request_index=0):
        rec.start(name); response=shared_http_transport.request("GET",target,timeout=(10,120)); response.raise_for_status()
        if len(response.content)!=args.response_bytes: raise RuntimeError("response length mismatch")
        rec.finish(name,response); return request_index
    return call

def run_runner(args,limits,rec):
    configure(args); target=url(args); started=time.perf_counter()
    with ThreadPoolExecutor(max_workers=22) as pool:
        fs=[]
        for name,count,limit in zip(NAMES,JOBS,limits):
            runner=ApiRunner(max_threads=limit,poll_interval=.005); fn=handler(name,target,args,rec)
            fs.append(pool.submit(runner.run_jobs,name,list(range(count)),lambda i,_f=fn:_f(None,i)))
        for f in as_completed(fs): f.result()
    elapsed=time.perf_counter()-started; snap=shared_http_transport.snapshot(); close_shared_http_transport(); return elapsed,snap,{}

def run_workflow(args,limits,rec):
    configure(args); target=url(args)
    with tempfile.TemporaryDirectory(prefix="mwf-network-manager-skew-", ignore_cleanup_errors=True) as d:
        wf=MicroWorkflow(Path(d),runner="api"); wf.active_job_restart_enabled=True; wf.graph([("fanout",n) for n in NAMES])
        source=NodeRouter("fanout",runner="threaded",max_threads=1); source.create_job(params={"seed":True})
        @source.task
        def fanout(ctx,seed):
            for name,count in zip(NAMES,JOBS): ctx.node(name).add_many([{"request_index":i} for i in range(count)])
            return seed
        wf.include_router(source); retained=[source]
        for name,limit in zip(NAMES,limits):
            router=NodeRouter(name,runner="api",max_threads=limit,timeout=180); router.task(timeout=180)(handler(name,target,args,rec)); wf.include_router(router); retained.append(router)
        started=time.perf_counter(); wf.run(); elapsed=time.perf_counter()-started; snap=shared_http_transport.snapshot(); close_shared_http_transport()
        wf.storage.flush_db_mutations(); persisted=wf.storage.network_manager_state()
        failed=sum(wf.storage.job_status_counts(n).get("failed",0) for n in NAMES)
        time.sleep(0.6)
        wf.storage.close_database_connections()
        if failed: raise RuntimeError(f"{failed} failed jobs")
        return elapsed,snap,persisted

def main():
    p=argparse.ArgumentParser(); p.add_argument("--endpoint",default="https://127.0.0.1:8766"); p.add_argument("--http2",action="store_true")
    p.add_argument("--architecture",choices=["manager","direct"],default="manager"); p.add_argument("--mode",choices=["runner","workflow"],default="workflow")
    p.add_argument("--concurrency",type=int,default=512); p.add_argument("--response-bytes",type=int,default=4096); p.add_argument("--bytes-per-second",type=int,default=0); p.add_argument("--delay-ms",type=float,default=5.0); p.add_argument("--json",default="")
    args=p.parse_args(); limits=weighted_split(args.concurrency,JOBS); rec=Recorder(); elapsed,snap,persisted=(run_runner if args.mode=="runner" else run_workflow)(args,limits,rec)
    tps={}; send=[]; recv=[]
    for name,count in zip(NAMES,JOBS):
        r=rec.data[name]; duration=max(1e-9,r["last"]-r["first"]); tps[name]=count/duration; send+=r["send"]; recv+=r["recv"]
    ratio=statistics.median(tps[n] for n in NAMES[:2])/statistics.median(tps[n] for n in NAMES[2:])
    out={"architecture":args.architecture,"mode":args.mode,"protocol":"h2" if args.http2 else "h1","jobs_total":sum(JOBS),"concurrency":args.concurrency,
         "limits":dict(zip(NAMES,limits)),"response_bytes":args.response_bytes,"bytes_per_second":args.bytes_per_second,"elapsed_seconds":elapsed,"jobs_per_second_total":sum(JOBS)/elapsed,
         "throughput_ratio_big_to_small_target_20":ratio,"throughput_ratio_score_ideal_1":ratio/20,
         "network_send_delay_p99_seconds":pct(send,.99),"network_send_delay_max_seconds":max(send or [0.0]),"network_receive_delay_p99_seconds":pct(recv,.99),"network_receive_delay_max_seconds":max(recv or [0.0]),
         "manager_snapshot":snap,"persisted_network_nodes":len(persisted)}
    text=json.dumps(out,indent=2,sort_keys=True); print(text)
    if args.json: Path(args.json).write_text(text+'\n')
if __name__=="__main__": main()
