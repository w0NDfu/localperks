import sys
from pathlib import Path
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2] / 'agent-service'))
from ops.reasoning import analyze
from ops.tools import ObservationScope,evidence
from ops.engine import Move


def item(tool,data,i=1,at=1000):
    return evidence('e'+str(i),tool,{'observedAt':at,'data':data})


def stock(db,redis,pending=0):
    return {'mysql':[{'stock':db}],'redisStock':str(redis),'pending':[{'total':pending}]}


def test_inflight_reservations_are_not_inventory_drift():
    result=analyze([item('inventory',stock(10,7,3),1),item('inventory',stock(10,7,3),2,2000)])
    assert 'REDIS_STOCK_DRIFT' not in result['supported']
    assert 'REDIS_STOCK_DRIFT' in result['excluded']


def test_one_snapshot_or_same_instant_cannot_establish_persistent_drift():
    assert 'REDIS_STOCK_DRIFT' not in analyze([item('inventory',stock(10,7))])['supported']
    assert 'REDIS_STOCK_DRIFT' not in analyze([item('inventory',stock(10,7)),item('inventory',stock(10,7),2,1001)])['supported']


def test_two_stable_snapshots_establish_difference_but_not_the_writer():
    result=analyze([item('inventory',stock(10,7),1),item('inventory',stock(10,7),2,2000)])
    proof=result['supported']['REDIS_STOCK_DRIFT']
    assert proof['evidence_ids']==['e1','e2'] and '成因尚无证据' in proof['detail']


def test_positive_lag_alone_does_not_prove_disabled_consumption():
    assert 'MQ_CONSUME_DISABLED' not in analyze([item('mq',{'lag':4,'consumeEnabled':True})])['supported']
    result=analyze([item('mq',{'lag':4,'consumeEnabled':False,'brokerOffset':5,'consumerOffset':1})])
    assert 'MQ_CONSUME_DISABLED' in result['supported']


def test_http429_does_not_prove_ip_instead_of_user_or_global_limit():
    data={'events':[{'type':'http','status':429,'limitScope':'user'}],'coverageComplete':True}
    assert 'IP_RATE_LIMIT' not in analyze([item('traffic',data)])['supported']
    data['events'][0]['limitScope']='ip'
    assert 'IP_RATE_LIMIT' in analyze([item('traffic',data)])['supported']


def test_duplicate_receive_requires_idempotency_and_database_evidence():
    events=[{'type':kind,'orderId':'123'} for kind in ['mq_received','mq_received','mq_handler_returned','mq_handler_returned']]
    data={'events':events,'orders':[{'orderId':'123'}],'coverageComplete':True}
    assert 'DUPLICATE_DELIVERY' not in analyze([item('delivery',data)])['supported']
    events.append({'type':'order_idempotent','orderId':'123'})
    assert 'DUPLICATE_DELIVERY' in analyze([item('delivery',data)])['supported']
    data['orders'].append({'orderId':'123'})
    assert 'DUPLICATE_DELIVERY' not in analyze([item('delivery',data)])['supported']


@pytest.mark.parametrize('change',[{'ip':'127.0.0.1; FLUSHALL'},{'end':2000000},{'voucher_id':-1},{'sql':'DELETE FROM orders'}])
def test_model_cannot_expand_scope_or_supply_sql(change):
    with pytest.raises(ValueError):ObservationScope.model_validate({'start':1000,'end':2000,'voucher_id':1,**change})


def test_error_and_missing_history_are_not_healthy():
    result=analyze([{'id':'e1','tool':'mq','error':'UNAVAILABLE'},item('events',{'coverageComplete':False})])
    assert 'NO_FAULT' not in result['supported'] and 'INSUFFICIENT_EVIDENCE' in result['supported']


def test_model_cannot_request_mutation_tools():
    with pytest.raises(ValueError):
        Move.model_validate({'hypotheses':[{'code':'INSUFFICIENT_EVIDENCE','reason':'needs evidence'}],
                             'tools':['shell'], 'reason':'try docker restart'})
