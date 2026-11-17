"""Recreate only isolated business fixtures to verify the diagnosed filter fix."""
import asyncio
import hashlib
import json
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parent))
from experiment import Experiment,ROOT
from model_budget import load,write
from usage import export


async def main(parent):
    parent=Path(parent).resolve()
    identifier=datetime.now().strftime('v03-%Y%m%d-%H%M%S-')+uuid.uuid4().hex[:8]
    path=ROOT/'artifacts/test-runs'/identifier;(path/'temp').mkdir(parents=True)
    (path/'.run-owner').write_text(identifier)
    write(path/'resource_manifest.json',{'run_id':identifier,'created_at':datetime.now().isoformat(),
        'compose_project':'sltest-'+identifier,'resources':[]})
    write(path/'authorization.json',{**load(parent/'authorization.json'),'parent_run':str(parent),
        'purpose':'Only diagnosed exclusive-filter fix; same uncapped user authorization, no stress or repeated batch'})
    experiment=Experiment(path)
    experiment.register('directory',str(path/'temp'),'Dedicated transient subtree')
    try:
        experiment.sample('before');experiment.build();await experiment.infrastructure()
        experiment.monitor_thread=threading.Thread(target=experiment.monitor,daemon=True);experiment.monitor_thread.start()
        await experiment.fixtures()
        case=next(c for c in load(parent/'agent_dataset.json')['cases'] if c['id']=='c_unknown_rule')
        write(path/'agent_dataset.json',{'cases':[case],'frozen_parent_dataset_sha256':hashlib.sha256((parent/'agent_dataset.json').read_bytes()).hexdigest()})
        await experiment.request(case,label='c_unknown_rule_final_schema_fix')
        item=experiment.results[case['id']];item['result']['evidence_run']=str(path)
        supplemental=load(parent/'supplemental_results.json',{});supplemental[case['id']]=item
        write(parent/'supplemental_results.json',supplemental)
        write(parent/'focused_recheck_run.json',{'run':str(path),'reason':'Exact duplicate keyword JSON normalization; unchanged frozen case/expectations'})
    except Exception as exc:
        experiment.failure=type(exc).__name__+': '+str(exc)
    finally:
        experiment.backup_tasks();export(path);experiment.cleanup()
        write(path/'experiment_state.json',{'failure':experiment.failure,'results':experiment.results})
        print(json.dumps({'run':str(path),'failure':experiment.failure,'results':{k:v['result'] for k,v in experiment.results.items()}},ensure_ascii=False),flush=True)


if __name__=='__main__':asyncio.run(main(sys.argv[1]))
