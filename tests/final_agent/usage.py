"""Per-experiment actual usage accounting. No legacy total-token or request cap."""
import asyncio
import contextvars
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx

CASE=contextvars.ContextVar('evaluation_case',default=os.getenv('EVAL_CASE','preparation'))


def connection(run):
    conn=sqlite3.connect(Path(run)/'usage.sqlite3',timeout=20)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY,case_id TEXT,kind TEXT,model TEXT,
      started REAL,duration_ms REAL,status INTEGER,input_bytes INTEGER,request_sha256 TEXT,
      input_tokens INTEGER,output_tokens INTEGER,total_tokens INTEGER,usage TEXT,error TEXT,output_limit INTEGER)''')
    conn.execute('CREATE TABLE IF NOT EXISTS flags(name TEXT PRIMARY KEY,value TEXT)')
    return conn


def rows(run,case=None):
    with connection(run) as conn:
        data=conn.execute('SELECT * FROM requests'+(' WHERE case_id=?' if case else '')+' ORDER BY id', (case,) if case else ()).fetchall()
    return [dict(row) for row in data]


def export(run):
    data=rows(run)
    result={'authorization':str(Path(run)/'authorization.json'),'total_token_cap':None,'actual_requests':len(data),
       'known_input_tokens':sum(r['input_tokens'] or 0 for r in data),'known_output_tokens':sum(r['output_tokens'] or 0 for r in data),
       'known_total_tokens':sum(r['total_tokens'] or 0 for r in data),'usage_unknown_requests':sum(r['total_tokens'] is None for r in data),
       'cost':'UNKNOWN','cost_reason':'No provider invoice or verified account billing rate','requests':data}
    (Path(run)/'api_usage.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result


class ActualApiTransport(httpx.AsyncBaseTransport):
    def __init__(self,run,output_limit=1500):
        self.run=Path(run)
        self.output_limit=output_limit

    async def handle_async_request(self,request):
        if urlparse(str(request.url)).hostname!='api.openlux.ai':raise RuntimeError('Unauthorized model endpoint')
        kind='chat' if request.url.path.endswith('/chat/completions') else 'embedding' if request.url.path.endswith('/embeddings') else 'rerank'
        payload=json.loads(request.content)
        if kind=='chat':
            payload.pop('max_completion_tokens',None)
            payload['max_tokens']=self.output_limit
        content=json.dumps(payload,ensure_ascii=False).encode()
        case=CASE.get()
        for attempt in range(2):
            with connection(self.run) as conn:
                conn.execute('BEGIN IMMEDIATE')
                if conn.execute("SELECT value FROM flags WHERE name='provider_blocked'").fetchone():raise RuntimeError('Provider credentials/quota failure already recorded')
                count=conn.execute('SELECT COUNT(*) FROM requests WHERE case_id=?',(case,)).fetchone()[0]
                if count>=18:raise RuntimeError('PER_TASK_REQUEST_LIMIT_18')
                identifier=conn.execute('INSERT INTO requests(case_id,kind,model,started,input_bytes,request_sha256,output_limit) VALUES(?,?,?,?,?,?,?)',
                    (case,kind,payload.get('model'),time.time(),len(content),hashlib.sha256(content).hexdigest(),self.output_limit if kind=='chat' else None)).lastrowid
            started=time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(60,connect=10),trust_env=False) as client:
                    response=await client.post(str(request.url),headers={'Authorization':request.headers['authorization'],'Content-Type':'application/json'},content=content)
                raw=response.json();usage=raw.get('usage') or {}
                # Preserve public output and tool arguments, not private reasoning content.
                folder=self.run/'model_responses';folder.mkdir(exist_ok=True)
                public={k:raw[k] for k in ('id','model','usage','results','object','error','cost','billed_units','meta') if k in raw}
                if 'choices' in raw:
                    public['choices']=[{'index':choice.get('index'),'finish_reason':choice.get('finish_reason'),
                        'message':{k:v for k,v in choice.get('message',{}).items() if k in ('role','content','tool_calls','refusal')}} for choice in raw['choices']]
                (folder/(str(identifier)+'.json')).write_text(json.dumps(public,ensure_ascii=False,indent=2),encoding='utf-8')
                prompt=usage.get('prompt_tokens');completion=usage.get('completion_tokens',0) if usage else None
                total=usage.get('total_tokens')
                if total is None and prompt is not None:total=prompt+(completion or 0)
                with connection(self.run) as conn:
                    conn.execute('UPDATE requests SET duration_ms=?,status=?,input_tokens=?,output_tokens=?,total_tokens=?,usage=? WHERE id=?',
                         ((time.perf_counter()-started)*1000,response.status_code,prompt,completion,total,json.dumps(usage),identifier))
                    if response.status_code in (401,402,403):
                        conn.execute("INSERT OR REPLACE INTO flags VALUES('provider_blocked',?)",(str(response.status_code),))
                if response.status_code in (429,502,503) and attempt==0:
                    await asyncio.sleep(1)
                    continue
                return httpx.Response(response.status_code,content=response.content,headers={'content-type':'application/json'},request=request)
            except BaseException as exc:
                with connection(self.run) as conn:
                    conn.execute('UPDATE requests SET duration_ms=?,error=? WHERE id=?',((time.perf_counter()-started)*1000,type(exc).__name__,identifier))
                raise
