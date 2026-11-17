import asyncio
import hashlib
import json
import statistics
import sqlite3
import sys
import time
from dataclasses import replace
from pathlib import Path

from model_budget import load,write
from usage import CASE,ActualApiTransport,export

ROOT=Path(__file__).resolve().parents[2]


QUESTIONS=[
 ('r01','已经付过款的订单，还能按未付款订单一样取消吗？',['取消订单与超时']),
 ('r02','别人知道我的订单编号，能帮我取消吗？',['登录、确认与隐私','取消订单与超时']),
 ('r03','未支付订单被取消后，预扣的库存会怎样？',['取消订单与超时']),
 ('r04','支付说明：这个系统会真实扣银行卡的钱吗？',['支付与售后']),
 ('r05','项目已经有真实退款和核销接口了吗？',['支付与售后']),
 ('r06','券开抢到停售的时段，等同我去店里消费的有效期吗？',['秒杀购买与使用规则']),
 ('r07','没有写明叠加规则，能把几张优惠券一起算优惠吗？',['秒杀购买与使用规则','推荐指标与消费估算']),
 ('r08','优惠券规则不明确，只写商家说了算，就能保证今晚用吗？',['秒杀购买与使用规则']),
 ('r09','店铺的人均参考价就是最后结账金额吗？',['推荐指标与消费估算']),
 ('r10','项目怎样估算一张券之后的实付？',['推荐指标与消费估算']),
 ('r11','接口说请求已经收到，是否就一定创建了订单？',['秒杀购买与使用规则','订单状态与异步查询']),
 ('r12','订单还在排队和处理中时，可以告诉用户抢券成功了吗？',['订单状态与异步查询']),
 ('r13','个人券包查询有什么登录和归属限制？',['登录、确认与隐私']),
 ('r14','Agent可以未经本人确认就抢券或取消订单吗？',['登录、确认与隐私']),
 ('r15','网络超时导致交易结果未知，应该立刻重复提交吗？',['登录、确认与隐私']),
 ('r16','当前店铺API能证明停车、包间或预约余量吗？',['店铺服务说明']),
 ('n17','验收餐厅9001今天包间订金具体多少元？',[]),
 ('n18','优惠券93001现在还剩多少库存？',[]),
 ('n19','店铺9002今天最新菜单价格是多少？',[]),
 ('n20','优惠券93003今晚确定能用吗？',[]),
 ('n21','优惠券93001最多适用几个人？',[]),
 ('n22','真实银行卡退款要多久到账，具体几天？',[]),
 ('n23','2027年春节杭州限行尾号是什么？',[]),
 ('n24','杭州明天的天气和航班情况怎样？',[]),
]


async def evaluate(experiment):
    # Apply the final interpreter/grounding fixes before the final acceptance replay.
    await experiment.start_agent()
    failed_recovery=experiment.results.get('fault_recover')
    if failed_recovery and failed_recovery['result']['status']=='FAIL':
        # JSON errors do not include the streamed task frame. Recover the known test task
        # from this run's own SQLite evidence, then use the actual authenticated resume API.
        with sqlite3.connect(experiment.env['AGENT_TASK_DB']) as conn:
            found=conn.execute("SELECT id FROM agent_tasks WHERE scope=? AND state='failed' AND json_extract(request,'$.message')=? ORDER BY created DESC LIMIT 1",
                ('user:10001',failed_recovery['case']['message'])).fetchone()
        if found:
            await experiment.request(failed_recovery['case'],{'message':'恢复','task_id':found[0]},label='fault_recover_resumed_verified')
            write(experiment.path/'fault_recovery_proof.json',{'task_id':found[0],'lookup':'Own isolated SQLite; original JSON response omitted SSE task frame',
                'resume_via_real_authenticated_api':True,'result':experiment.results['fault_recover']['result']})
    core_cases=[experiment.results[key]['case'] for key in ('A_complete','B1','B2','B3','B4','B5')]
    if any(experiment.results[key]['result']['status']!='PASS' for key in ('A_complete','B1','B2','B3','B4','B5')):
        experiment.stage('final-live-core-acceptance-after-schema-fix')
        if experiment.results['A_complete']['result']['status']!='PASS':
            await experiment.request(core_cases[0],label='A_complete_acceptance')
        session=None
        for case in core_cases[1:]:
            body={'message':case['message']}
            if case.get('ui'):body['constraints']=case['ui']
            if session:body['session_id']=session
            data,_=await experiment.request(case,body,label=case['id']+'_acceptance')
            session=data.get('session_id') or session
        write(experiment.path/'continuous_dialogue_acceptance.json',{
           'session_id':session,'turns':[{k:experiment.results[key]['output'].get(k) for k in ('session_id','task_id','constraints','answer_origin','decision_context')} for key in ('B1','B2','B3','B4','B5')],
           'all_real_model_passed':all(experiment.results[key]['result']['status']=='PASS' for key in ('B1','B2','B3','B4','B5'))})
    # Local failures get one diagnosed-scope recheck after fixes, never an unlimited rerun.
    for item in list(experiment.results.values()):
        case=item['case']
        if item['result']['status']=='FAIL' and case['id'] not in ('A_complete','B1','B2','B3','B4','B5','fault_recover','checkpoint_resume','stock_zero') and case['kind']!='rag':
            await experiment.request(case,label=case['id']+'_after_fix')
    experiment.stage('real-rag-sufficiency-24-frozen-queries')
    experiment.sample('before-real-rag',guard=True)
    from core.config import load_settings
    from rag.ingestion import chunks
    from rag.store import PgVectorStore
    from rag.retrieval import EmbeddingClient,Reranker,RagService
    from rag.lexical import BM25,rrf
    from rag.evidence import assess
    experiment.create('pgvector','pgvector/pgvector:pg16',['--memory','384m','--memory-swap','384m',
       '--tmpfs','/var/lib/postgresql/data:rw,size=128m','-p',f'127.0.0.1:{experiment.pg_port}:5432',
       '-e','POSTGRES_PASSWORD='+experiment.mysql_password,'-e','POSTGRES_DB=final_rag'],
       ['postgres','-c','shared_buffers=32MB','-c','max_connections=15'])
    dsn=f'postgresql://postgres:{experiment.mysql_password}@127.0.0.1:{experiment.pg_port}/final_rag'
    experiment.env['PGVECTOR_DSN']=dsn
    cfg=replace(load_settings(),pgvector_dsn=dsn);store=PgVectorStore(dsn,cfg.embedding_dimensions)
    for _ in range(20):
        try:await store.initialize();break
        except Exception:await asyncio.sleep(1)
    else:raise RuntimeError('Dedicated pgvector startup failed')
    source=ROOT/'docs/AGENT_BUSINESS_KNOWLEDGE.md';documents=[]
    for part in source.read_text(encoding='utf-8').split('\n## ')[1:]:
        title,text=part.split('\n',1)
        documents.extend(chunks(text,'repo:docs/AGENT_BUSINESS_KNOWLEDGE.md#'+title,title=title,size=600,overlap=80))
    cases=[{'id':identifier,'query':query,'relevant_titles':titles,'expected_abstain':not titles} for identifier,query,titles in QUESTIONS]
    write(experiment.path/'rag_frozen_dataset.json',{'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'annotation':'Manually checked source sections, frozen before API calls; no post-result relevance changes',
        'documents':[d.to_dict() for d in documents],'cases':cases})
    transport=ActualApiTransport(experiment.path);embedding=EmbeddingClient(cfg,transport)
    token=CASE.set('rag_index')
    try:
        begin=time.perf_counter();vectors,usage=await embedding.embed([d.text for d in documents]);doc_ms=(time.perf_counter()-begin)*1000
        await store.upsert_document(documents,vectors,cfg.embedding_model)
        begin=time.perf_counter();queries,query_usage=await embedding.embed([c['query'] for c in cases]);query_ms=(time.perf_counter()-begin)*1000
    finally:CASE.reset(token)
    write(experiment.path/'rag_embedding_usage.json',{'document_usage':usage,'query_usage':query_usage,'document_ms':doc_ms,'query_batch_ms':query_ms})
    results=[];answers=[];gate=asyncio.Semaphore(2)
    async def one(case,vector):
        async with gate,asyncio.timeout(220):
            tag=CASE.set('rag_'+case['id'])
            try:
                start=time.perf_counter();sparse=BM25([d.to_dict() for d in documents]).search(case['query'],20);sparse_ms=(time.perf_counter()-start)*1000
                start=time.perf_counter();dense=await store.dense(vector,cfg.embedding_model,20);dense_ms=(time.perf_counter()-start)*1000
                start=time.perf_counter();fused=rrf(sparse,dense);rrf_ms=(time.perf_counter()-start)*1000
                no_relevance=not sparse and (not dense or dense[0]['distance']>.45)
                hybrid=[] if no_relevance else fused[:5]
                start=time.perf_counter();ranked,backend,warning=await Reranker(cfg,transport).rank(case['query'],[] if no_relevance else fused,5);rank_ms=(time.perf_counter()-start)*1000
                relevant={d.id for d in documents if d.title in case['relevant_titles']}
                rows=[]
                for mode,hits,elapsed in [('bm25',sparse[:5],sparse_ms),('dense',dense[:5],dense_ms),
                    ('hybrid',hybrid,sparse_ms+dense_ms+rrf_ms),('rerank',ranked,sparse_ms+dense_ms+rrf_ms+rank_ms)]:
                    ids=[h['id'] for h in hits];rank=next((i for i,item in enumerate(ids,1) if item in relevant),None);suff=assess(case['query'],hits)
                    for hit in hits:
                        actual=await store.source(hit['id']);assert actual['text']==hit['text']
                    rows.append({'case_id':case['id'],'mode':mode,'answerable':bool(relevant),'recall_at_5':len(set(ids)&relevant)/len(relevant) if relevant else None,
                        'mrr':1/rank if rank else 0 if relevant else None,'citation_precision_at_5':len(set(ids)&relevant)/len(ids) if ids else (0 if relevant else 1),
                        'raw_retrieval_abstained':not bool(ids),'sufficiency_abstained':not suff['sufficient'],
                        'correct_answerability':(not suff['sufficient'])==case['expected_abstain'],
                        'retrieval_ms_excluding_shared_embedding':elapsed,'rerank_backend':backend if mode=='rerank' else ''})
                trace=[]
                async def emit(event):trace.append(event)
                service=RagService(cfg,store=store,embedding=embedding,reranker=Reranker(cfg,transport),model_transport=transport)
                actual_result={'hits':ranked,'strategy':'bm25+dense+rrf+'+backend,'duration_ms':sparse_ms+dense_ms+rrf_ms+rank_ms,'embedding_usage':{},'warning':warning}
                # Static KB snapshots are reused; generation still invokes the real model.
                start=time.perf_counter();answer=await service.answer_from_retrieval(case['query'],actual_result,emit);answer_ms=(time.perf_counter()-start)*1000
                errors=[]
                if answer.get('answer_origin')!='llm_grounded':errors.append('Final answer not real grounded LLM')
                if answer.get('abstained')!=case['expected_abstain']:errors.append('Wrong refusal/answerability')
                facts=answer.get('answer_evidence',{});rendered=answer.get('answer_composition','')
                import re
                for key in re.findall(r'\{\{(\w+)\}\}',rendered):rendered=rendered.replace('{{'+key+'}}',str(facts[key]))
                if rendered!=answer['answer']:errors.append('Ungrounded final prose')
                entry={'id':case['id'],'status':'PASS' if not errors else 'FAIL','failures':errors,'latency_ms':answer_ms,'abstained':answer.get('abstained')}
                write(experiment.path/('rag-'+case['id']+'.json'),{'case':case,'retrieval':actual_result,'modes':rows,'answer':answer,'trace':trace,'evaluation':entry})
                experiment.stage('rag-'+case['id']+'-'+entry['status'])
                return rows,entry
            finally:CASE.reset(tag)
    evaluated=await asyncio.gather(*(one(c,v) for c,v in zip(cases,queries)),return_exceptions=True)
    for case,outcome in zip(cases,evaluated):
        if isinstance(outcome,Exception):
            entry={'id':case['id'],'status':'FAIL','failures':[type(outcome).__name__+': '+str(outcome)]};answers.append(entry)
            write(experiment.path/('rag-'+case['id']+'-failure.json'),entry)
        else:results.extend(outcome[0]);answers.append(outcome[1])
    write(experiment.path/'rag_results.json',{'retrieval_rows':results,'answers':answers});export(experiment.path)
