"""One protocol diagnosis using previously captured genuine Java observations."""
import asyncio,json,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE.parents[1]/'agent-service')]
from core.config import load_settings
from ops.engine import Investigator
from ops.reasoning import analyze
from ops.tools import ObservationScope
from ops.usage import UsageTransport,INVESTIGATION,usage_rows


async def main(path):
    path=Path(path);data=json.loads((path/'live-stock-recheck.json').read_text(encoding='utf-8'))
    observations=data['output']['observations'];scope=ObservationScope.model_validate(data['request']['scope'])
    transport=UsageTransport(str(path/'protocol-usage.sqlite3'));tag=INVESTIGATION.set({'task_id':'protocol-probe','requests':0})
    trace=[]
    async def emit(event):trace.append(event)
    try:
        checks=analyze(observations);checks['supported'].pop('INSUFFICIENT_EVIDENCE',None)
        result=await Investigator(load_settings(),None,None,transport)._move(data['request']['symptom'],scope,observations,checks,emit,'Collect missing evidence with the tools array.')
        print(result.model_dump(),flush=True)
    except Exception as exc:print(type(exc).__name__,str(exc),flush=True)
    finally:
        INVESTIGATION.reset(tag);await transport.inner.aclose()
        (path/'protocol-probe.json').write_text(json.dumps({'trace':trace,'usage':usage_rows(path/'protocol-usage.sqlite3')},ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':asyncio.run(main(sys.argv[1]))
