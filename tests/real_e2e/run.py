"""Real isolated LocalPerks validation. No simulated business, embeddings or responses."""
import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from redis.asyncio import Redis

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
AGENT = ROOT / 'agent-service'
sys.path[:0] = [str(AGENT), str(HERE)]
from core.config import load_settings
from model_budget import RealBudgetTransport, load, write


def csv_save(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with Path(path).open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pct(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)] if values else None


USED_PORTS = set()


def free_port(pair=False):
    # Avoid the Windows dynamic outbound range and reserve unique selections,
    # including the RocketMQ VIP port (listenPort-2).
    for _ in range(200):
        candidate = 20000 + secrets.randbelow(24000)
        ports = [candidate, candidate-2] if pair else [candidate]
        if any(port in USED_PORTS for port in ports):
            continue
        sockets=[]
        try:
            for port in ports:
                sock=socket.socket();sockets.append(sock)
                sock.bind(('127.0.0.1',port))
            USED_PORTS.update(ports)
            return candidate
        except OSError:
            pass
        finally:
            for sock in sockets:sock.close()
    raise RuntimeError('No safe distinct test ports available')


class Run:
    def __init__(self):
        self.id = datetime.now().strftime('v03-%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8]
        self.path = ROOT / 'artifacts/test-runs' / self.id
        (self.path / 'temp').mkdir(parents=True)
        (self.path / '.run-owner').write_text(self.id)
        self.project = 'sltest-' + self.id
        self.manifest = {'run_id': self.id, 'created_at': datetime.now().isoformat(), 'compose_project': self.project, 'resources': []}
        self.register('directory', str(self.path / 'temp'), 'Dedicated transient subtree')
        self.env = dict(os.environ, TEMP=str(self.path / 'temp'), TMP=str(self.path / 'temp'),
                        PYTHONDONTWRITEBYTECODE='1', PYTHONIOENCODING='utf-8')
        self.containers = {}
        self.processes = []
        self.files = []
        self.stop_monitor = threading.Event()
        self.sample_lock = threading.Lock()
        self.failure = None
        self.mysql_password = secrets.token_hex(20)
        self.java_rows = []
        self.agent_rows = []
        self.rag_rows = []
        write(self.path / 'api_usage.json', {'limits': {'chat_requests': 8, 'embedding_requests': 2, 'rerank_requests': 8, 'total_tokens': 50000},
              'known_tokens': 0, 'pending': False, 'closed': None, 'calls': [], 'blocked': [],
              'chat_soft_token_cap': 40000, 'note': '10k reserved for RAG. Per-request reservation is an estimate, never a provider hard cap.'})

    def register(self, kind, identifier, method, **extra):
        self.manifest['resources'].append({'type': kind, 'identifier': identifier, 'name': identifier,
            'run_id': self.id, 'created_at': datetime.now().isoformat(), 'compose_project': self.project,
            'creation_method': method, 'auto_cleanup': True, 'isolated': True, 'cleanup_status': 'pending', **extra})
        write(self.path / 'resource_manifest.json', self.manifest)

    def command(self, args, *, input=None, timeout=40, check=True, env=None):
        result = subprocess.run([str(a) for a in args], input=input, capture_output=True,
                                encoding='utf-8', errors='replace', timeout=timeout, cwd=ROOT, env=env or self.env)
        if check and result.returncode:
            # Do not expose environment or command arguments containing credentials.
            raise RuntimeError(f'{Path(str(args[0])).name} exited {result.returncode}: {result.stderr[-1200:]}')
        return result

    def stage(self, text):
        print(json.dumps({'stage': text, 'run_id': self.id}), flush=True)
        with (self.path / 'stages.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps({'stage': text, 'timestamp': datetime.now().isoformat()}) + '\n')

    def sample(self, phase, guard=False):
        with self.sample_lock:
            result = self.command(['pwsh', '-NoProfile', '-File', ROOT / 'scripts/test_resources.ps1',
                                  '-RunDirectory', self.path, '-Phase', phase, *([] if phase in ('before','end','after-cleanup') else ['-Quick'])], timeout=35, check=False)
            if guard:
                if result.returncode:
                    raise RuntimeError('Resource monitor unavailable')
                latest = load(self.path / 'resource_usage.json')['snapshots'][-1]
                if latest['available_memory_bytes'] is None or latest['available_memory_bytes'] < 2 * 1024**3:
                    raise RuntimeError('Available Windows RAM below 2 GiB or unknown')
                if latest['run_bytes'] > 1024**3 or shutil.disk_usage('D:/').free < 5 * 1024**3:
                    raise RuntimeError('Artifact/disk budget exceeded')
                total = 0
                for line in latest['docker_stats']:
                    stat = json.loads(line)
                    text = stat['MemUsage'].split('/')[0].strip()
                    match = re.match(r'([\d.]+)([KMG]?i?B)', text)
                    if match:
                        total += float(match[1]) * {'B':1,'kB':1000,'MiB':1024**2,'GiB':1024**3,'MB':10**6,'GB':10**9}.get(match[2],1024)
                if total > 6 * 1024**3:
                    raise RuntimeError('Docker sampled total above 6 GiB')
                for identifier in self.containers.values():
                    state = json.loads(self.command(['docker','inspect','--format','{{json .State}}',identifier]).stdout)
                    if not state['Running'] or state['OOMKilled']:
                        raise RuntimeError('Dedicated container unhealthy or OOM-killed')

    def monitor(self):
        while not self.stop_monitor.wait(10):
            try:
                self.sample('running')
            except Exception as exc:
                with (self.path / 'monitor_errors.log').open('a') as stream:
                    stream.write(type(exc).__name__ + '\n')

    def create(self, role, image, options, command, config=None):
        self.command(['docker','image','inspect',image])
        args = ['docker','create','--name',self.project+'-'+role,'--network',self.network,
                '--network-alias',role,'--label','localperks.validation.run='+self.id,
                '--label','com.docker.compose.project='+self.project,'--cpus','1'] + options + [image] + command
        identifier = self.command(args).stdout.strip()
        self.register('container', identifier, 'Cached image; private network; tmpfs; no persistent/bind mounts')
        self.containers[role] = identifier
        if config:
            self.command(['docker','cp',config,identifier+':/tmp/broker.conf'])
        self.command(['docker','start',identifier])
        return identifier

    def sql(self, statement, database='dianping_test'):
        result = self.command(['docker','exec','-i',self.containers['mysql'],'sh','-c',
             'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot --default-character-set=utf8mb4 -N -B --raw ' + database],
             input=statement, timeout=30)
        return [line.split('\t') for line in result.stdout.strip().splitlines() if line]

    def sql_json(self, statement):
        return [json.loads(line[0]) for line in self.sql(statement)]

    def admin(self, args, name):
        result = self.command(['docker','exec','-e','JAVA_OPT_EXT=-Xms32m -Xmx128m -XX:MaxDirectMemorySize=64m',
                               self.containers['broker'],'sh','mqadmin',*args], timeout=35, check=False)
        (self.path / (name+'.log')).write_text(result.stdout + result.stderr, encoding='utf-8')
        return result

    def build(self):
        self.stage('build-config-isolation-fix')
        result = self.command([shutil.which('mvn.cmd'),'-o','-B','-DskipTests','package'], timeout=150,
                              env=dict(self.env, MAVEN_OPTS='-Xms64m -Xmx512m'))
        (self.path/'maven-package.log').write_text(result.stdout+result.stderr, encoding='utf-8')
        self.jar = ROOT / 'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        write(self.path/'versions.json', {'git_head':self.command(['git','rev-parse','HEAD']).stdout.strip(),
             'java':self.command(['java','-version']).stderr.strip(), 'python':sys.version,
             'jar_sha256':hashlib.sha256(self.jar.read_bytes()).hexdigest(),
             'baseline_sha256':hashlib.sha256((AGENT/'evaluations/baseline_v02.zip').read_bytes()).hexdigest(),
             'normal_volumes_before':self.command(['docker','volume','ls','--format','{{.Name}}']).stdout.splitlines()})

    async def infrastructure(self):
        self.stage('isolated-dependencies')
        self.network = self.command(['docker','network','create','--label','localperks.validation.run='+self.id,
            '--label','com.docker.compose.project='+self.project,self.project]).stdout.strip()
        self.register('network',self.network,'Dedicated validation bridge, no external workloads')
        self.mysql_port, self.redis_port, self.namesrv_port = free_port(),free_port(),free_port()
        self.broker_port = free_port(pair=True)
        self.java_port, self.agent_port, self.pg_port = free_port(),free_port(),free_port()
        self.create('mysql','mysql:8.0', ['--memory','768m','--memory-swap','768m',
            '--tmpfs','/var/lib/mysql:rw,size=512m,mode=1777','-p',f'127.0.0.1:{self.mysql_port}:3306',
            '-e','MYSQL_ROOT_PASSWORD='+self.mysql_password,'-e','MYSQL_DATABASE=dianping_test','-e','TZ=Asia/Shanghai'],
            ['--innodb-buffer-pool-size=64M','--innodb-log-file-size=16M','--max-connections=60','--skip-log-bin','--performance-schema=OFF'])
        self.create('redis','redis:7.2-alpine',['--memory','128m','--memory-swap','128m',
            '--tmpfs','/data:rw,size=32m','-p',f'127.0.0.1:{self.redis_port}:6379'],
            ['redis-server','--save','','--appendonly','no','--maxmemory','64mb','--maxmemory-policy','noeviction'])
        self.create('namesrv','apache/rocketmq:5.3.2',['--memory','384m','--memory-swap','384m',
            '--tmpfs','/home/rocketmq/logs:rw,size=32m,mode=1777','-p',f'127.0.0.1:{self.namesrv_port}:9876',
            '-e','JAVA_OPT_EXT=-Xms64m -Xmx128m -Xmn32m -XX:MaxDirectMemorySize=64m'],['sh','mqnamesrv'])
        broker_config = self.path/'temp/broker.conf'
        broker_config.write_text(f'''brokerClusterName=ValidationCluster
brokerName=validation-broker
brokerId=0
brokerIP1=127.0.0.1
brokerIP2=127.0.0.1
listenPort={self.broker_port}
namesrvAddr=namesrv:9876
autoCreateTopicEnable=true
autoCreateSubscriptionGroup=true
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
mappedFileSizeCommitLog=16777216
mappedFileSizeConsumeQueue=300000
diskMaxUsedSpaceRatio=95
fileReservedTime=1
''', encoding='utf-8')
        self.create('broker','apache/rocketmq:5.3.2',['--memory','768m','--memory-swap','768m',
            '--tmpfs','/home/rocketmq/store:rw,size=256m,mode=1777','--tmpfs','/home/rocketmq/logs:rw,size=32m,mode=1777',
            '-p',f'127.0.0.1:{self.broker_port}:{self.broker_port}','-p',f'127.0.0.1:{self.broker_port-2}:{self.broker_port-2}',
            '-e','JAVA_OPT_EXT=-Xms128m -Xmx256m -Xmn64m -XX:MaxDirectMemorySize=128m'],
            ['sh','mqbroker','-c','/tmp/broker.conf'], config=broker_config)
        self.redis_url = f'redis://127.0.0.1:{self.redis_port}/0'
        self.java_url = f'http://127.0.0.1:{self.java_port}'
        self.agent_url = f'http://127.0.0.1:{self.agent_port}'
        for _ in range(45):
            try:
                self.sql('SELECT 1;')
                break
            except Exception:
                await asyncio.sleep(2)
        else:
            raise RuntimeError('Isolated MySQL startup timeout')
        self.stage('fresh-schema-synthetic-business-fixtures')
        schema = (ROOT/'src/main/resources/db/dianping.sql').read_text(encoding='utf-8').split('INSERT INTO')[0]
        schema = schema.replace('`dianping`','`dianping_test`')
        schema = '\n'.join(line for line in schema.splitlines() if not line.startswith('DROP TABLE'))
        self.sql(schema)
        self.sql("INSERT INTO tb_shop_type(id,name,sort) VALUES(1,'Food',1);")
        for i in range(1,4):
            self.sql(f"INSERT INTO tb_shop(id,name,type_id,address,x,y,avg_price,open_hours) VALUES({9000+i},'验收餐厅{i}',1,'合成测试地址',{120.16+i*.0001},{30.25+i*.0001},{[80,90,60][i-1]},'10:00-22:00');")
        values = ','.join(f"({10000+i},'199{str(i).zfill(8)}','验收账号{i}')" for i in range(1,101))
        self.sql('INSERT INTO tb_user(id,phone,nick_name) VALUES'+values+';')
        self.tokens = {10000+i:self.id+'-user-'+str(i) for i in range(1,101)}
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:
            for uid, token in self.tokens.items():
                await redis.hset('login:token:'+token,mapping={'id':str(uid),'nickName':'验收账号'+str(uid-10000)})
                await redis.expire('login:token:'+token,3600)
            for i in range(1,4):
                await redis.geoadd('shop:geo:1',[120.16+i*.0001,30.25+i*.0001,str(9000+i)])
        for _ in range(20):
            result = self.admin(['clusterList','-n','namesrv:9876'],'mq-startup')
            if 'validation-broker' in result.stdout:
                break
            await asyncio.sleep(2)
        else:
            raise RuntimeError('Isolated broker registration timeout')
        topics=['seckill-order-topic','voucher-order-timeout-topic','shop-cache-invalidation-topic','voucher-cache-invalidation-topic',
                '%DLQ%hmdp-seckill-order-consumer','%DLQ%hmdp-order-timeout-consumer']
        for i,topic in enumerate(topics):
            result=self.admin(['updateTopic','-n','namesrv:9876','-b',f'127.0.0.1:{self.broker_port}','-t',topic,'-r','2','-w','2'],f'mq-create-topic-{i}')
            if result.returncode:
                raise RuntimeError('Topic creation failed')
        self.env.update(HMDP_DB_URL=f'jdbc:mysql://127.0.0.1:{self.mysql_port}/dianping_test?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true',
                        HMDP_DB_USERNAME='root', HMDP_DB_PASSWORD=self.mysql_password,
                        SHOPLINK_BASE_URL=self.java_url,REDIS_URL=self.redis_url,
                        AGENT_TASK_DB=str(self.path/'temp/agent-tasks.sqlite3'))
        java_log=self.path/'java.log'
        stream=java_log.open('w',encoding='utf-8');self.files.append(stream)
        java=subprocess.Popen(['java','-Xms64m','-Xmx512m','-XX:MaxDirectMemorySize=128m','-Dfile.encoding=UTF-8',
             *getattr(self,'java_vm_options',()),
             '-Duser.home='+str(self.path/'temp/java-home'),'-Drocketmq.client.logRoot='+str(self.path/'temp/mq-client-logs'),'-jar',str(self.jar),
             '--server.address=127.0.0.1',f'--server.port={self.java_port}',f'--spring.redis.port={self.redis_port}',
             '--spring.redis.host=127.0.0.1',f'--rocketmq.name-server=127.0.0.1:{self.namesrv_port}',
             '--logging.level.com.hmdp=INFO','--logging.level.com.hmdp.mq=DEBUG','--rocketmq.consumer.consume-thread-max=4',
             '--rocketmq.consumer.consume-thread-number=2',*getattr(self,'java_app_options',())],cwd=ROOT,env=self.env,stdout=stream,stderr=subprocess.STDOUT)
        self.processes.append(java)
        async with httpx.AsyncClient(timeout=3,trust_env=False) as client:
            for _ in range(65):
                if java.poll() is not None:
                    raise RuntimeError('Java startup failed; inspect java.log')
                try:
                    response=await client.get(self.java_url+'/shop-type/list')
                    if response.status_code==200 and response.json().get('success'):
                        break
                except Exception:
                    pass
                await asyncio.sleep(2)
            else:
                raise RuntimeError('Java readiness timeout')
        write(self.path/'isolation.json',{'java_url':self.java_url,'agent_url':self.agent_url,
             'redis_port':self.redis_port,'mysql_port':self.mysql_port,'nameserver_port':self.namesrv_port,'broker_port':self.broker_port,
             'topics':topics,'data_origin':'new empty test instance + synthetic fixtures; normal DB/Redis/MQ never accessed',
             'configuration_fix':'Redisson now follows Spring RedisProperties; normal 6379 stays stopped',
             'container_limits_mib':{'mysql':768,'redis':128,'namesrv':384,'broker':768},'native_java_heap_mib':512})

    async def coupon(self, identifier, stock, *, shop=9001, rules='长期有效；全天可用；每桌限1张；2人可用', mysql_stock=None, future=False, pay=6000):
        begin="DATE_ADD(NOW(), INTERVAL 1 DAY)" if future else "DATE_SUB(NOW(), INTERVAL 1 DAY)"
        self.sql(f"INSERT INTO tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) VALUES({identifier},{shop},'专项验收券{identifier}','{rules}',{pay},10000,1,1); INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES({identifier},{stock if mysql_stock is None else mysql_stock},{begin},DATE_ADD(NOW(),INTERVAL 7 DAY));")
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:
            await redis.set('seckill:stock:'+str(identifier),stock)

    async def clear_stage_window(self):
        # Only exact test-created limiter keys in the entire dedicated instance.
        keys=['rate:sliding:{voucher-seckill}:global:all','rate:sliding:{voucher-seckill}:ip:127.0.0.1']
        keys += ['rate:sliding:{voucher-seckill}:user:'+str(uid) for uid in self.tokens]
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:
            await redis.delete(*keys)
        with (self.path/'limiter-window-resets.jsonl').open('a') as stream:
            stream.write(json.dumps({'keys':keys,'instance':self.containers['redis'],'reason':'Independent wave window; per-wave limits unchanged'})+'\n')

    async def snapshot_voucher(self, voucher):
        rows=self.sql_json(f"SELECT JSON_OBJECT('id',id,'user_id',user_id,'voucher_id',voucher_id,'status',status) FROM tb_voucher_order WHERE voucher_id={voucher} ORDER BY id;")
        messages=self.sql_json(f"SELECT JSON_OBJECT('order_id',order_id,'status',status,'message_id',message_id,'retry_count',retry_count,'last_error',last_error,'topic',topic) FROM tb_mq_message WHERE voucher_id={voucher} ORDER BY id;")
        mq=[item for item in messages if item['topic']=='seckill-order-topic']
        timeouts=[item for item in messages if item['topic']=='voucher-order-timeout-topic']
        stock=int(self.sql(f'SELECT stock FROM tb_seckill_voucher WHERE voucher_id={voucher};')[0][0])
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:
            return {'voucher_id':voucher,'mysql_stock':stock,'orders':rows,'outbox':mq,'timeout_outbox':timeouts,
                    'redis_stock':int(await redis.get('seckill:stock:'+str(voucher))),
                    'redis_qualified_users':sorted(await redis.smembers('seckill:order:'+str(voucher)))}

    async def wait_orders(self, voucher, expected, failed=False):
        for _ in range(45):
            snap=await self.snapshot_voucher(voucher)
            if failed:
                if snap['outbox'] and all(m['status']==2 for m in snap['outbox']):
                    return snap
            elif len(snap['orders'])==expected and len(snap['outbox'])==expected and all(m['message_id'] for m in snap['outbox']):
                return snap
            await asyncio.sleep(.5)
        raise RuntimeError(f'Order convergence timeout voucher={voucher}, expected={expected}')

    async def java_tests(self):
        self.stage('real-java-seckill')
        async with httpx.AsyncClient(base_url=self.java_url,timeout=15,trust_env=False,limits=httpx.Limits(max_connections=100,max_keepalive_connections=20)) as client:
            for concurrency in (1,10,20,50,100):
                self.sample('before-java-'+str(concurrency),guard=True)
                await self.clear_stage_window()
                voucher=91000+concurrency
                initial=2 if concurrency==1 else concurrency//2
                await self.coupon(voucher,initial)
                async def request(uid):
                    start=time.perf_counter()
                    result=await client.post('/voucher-order/seckill/'+str(voucher),headers={'authorization':self.tokens[uid]})
                    return {'uid':uid,'http_status':result.status_code,'body':result.json() if result.content else None,
                            'duration_ms':(time.perf_counter()-start)*1000}
                start=time.perf_counter()
                responses=await asyncio.gather(*(request(10001+i) for i in range(concurrency)))
                request_seconds=time.perf_counter()-start
                accepted=[r for r in responses if r['http_status']==200 and r['body'].get('success')]
                snapshot=await self.wait_orders(voucher,len(accepted))
                ids=[str(r['body']['data']['orderId']) for r in accepted]
                assert len(snapshot['orders'])==len(set(ids))==len(accepted)
                assert snapshot['mysql_stock']==snapshot['redis_stock']==initial-len(accepted)>=0
                assert len(snapshot['redis_qualified_users'])==len(accepted)
                assert len({o['user_id'] for o in snapshot['orders']})==len(accepted)
                states=[]
                for row in accepted:
                    response=await client.get('/voucher-order/status/'+str(row['body']['data']['orderId']),headers={'authorization':self.tokens[row['uid']]})
                    state=response.json()
                    assert state['success'] and state['data']['statusCode']==12
                    states.append(state)
                if concurrency==1:
                    duplicate=await request(10001)
                    assert duplicate['body']['success'] and str(duplicate['body']['data']['orderId'])==ids[0]
                    after=await self.snapshot_voucher(voucher)
                    assert len(after['orders'])==1 and after['redis_stock']==1
                    write(self.path/'same-user-deduplication.json',{'initial':snapshot,'duplicate_http':duplicate,'after':after})
                    self.replay_order_id=ids[0]
                if concurrency==10:
                    self.agent_order=accepted[0]['body']['data']['orderId']
                    self.agent_user=accepted[0]['uid']
                progress=self.admin(['consumerProgress','-n','namesrv:9876','-g','hmdp-seckill-order-consumer'],f'consumer-progress-{concurrency}')
                row={'concurrency':concurrency,'http_requests':concurrency,'http_accepted':len(accepted),
                     'http_429':sum(r['http_status']==429 for r in responses),'http_5xx':sum(r['http_status']>=500 for r in responses),
                     'lua_qualified':len(snapshot['redis_qualified_users']),'final_mysql_orders':len(snapshot['orders']),
                     'initial_stock':initial,'mysql_remaining_stock':snapshot['mysql_stock'],'redis_remaining_stock':snapshot['redis_stock'],
                     'request_rps':concurrency/request_seconds,'p50_ms':pct([r['duration_ms'] for r in responses],.5),
                     'p95_ms':pct([r['duration_ms'] for r in responses],.95),'convergence_ms':(time.perf_counter()-start)*1000,
                     'outbox_sent_with_message_id':sum(bool(m['message_id']) and m['status']==1 for m in snapshot['outbox']),
                     'consumer_progress_command_success':progress.returncode==0,'status':'PASS'}
                self.java_rows.append(row)
                csv_save(self.path/'java_performance.csv',self.java_rows)
                write(self.path/f'java-wave-{concurrency}.json',{'responses':responses,'final_state':snapshot,'order_http_states':states})
                self.stage('java-wave-complete-'+str(concurrency))
            # Failure after successful Lua/MQ acceptance: MySQL stock is deliberately zero.
            await self.clear_stage_window()
            await self.coupon(92001,1,mysql_stock=0)
            response=await client.post('/voucher-order/seckill/92001',headers={'authorization':self.tokens[10001]})
            failure=await self.wait_orders(92001,0,failed=True)
            assert response.json()['success'] and not failure['orders'] and failure['mysql_stock']==0
            assert failure['redis_stock']==1 and not failure['redis_qualified_users']
            status=await client.get('/voucher-order/status/'+str(response.json()['data']['orderId']),headers={'authorization':self.tokens[10001]})
            assert status.json()['data']['statusCode']==15
            write(self.path/'mysql-failure-compensation.json',{'injected_initial':{'mysql_stock':0,'redis_stock':1},'accepted_http':response.json(),'final':failure,'http_status':status.json()})
            # Eligibility rejection must compensate the Lua reservation before any outbox insertion.
            await self.coupon(92002,1,future=True)
            response=await client.post('/voucher-order/seckill/92002',headers={'authorization':self.tokens[10001]})
            future=await self.snapshot_voucher(92002)
            assert not response.json()['success'] and future['redis_stock']==future['mysql_stock']==1 and not future['orders'] and not future['outbox']
            write(self.path/'activity-failure-compensation.json',{'http':response.json(),'final':future})
            await self.replay()
            # Cancel the dedicated single-user order, then repeat; stock restored only once.
            before=await self.snapshot_voucher(91001)
            cancelled=(await client.post('/voucher-order/cancel/'+self.replay_order_id,headers={'authorization':self.tokens[10001]})).json()
            repeated=(await client.post('/voucher-order/cancel/'+self.replay_order_id,headers={'authorization':self.tokens[10001]})).json()
            after=await self.snapshot_voucher(91001)
            assert cancelled['data']['statusCode']==repeated['data']['statusCode']==14
            assert after['mysql_stock']==after['redis_stock']==2 and after['orders'][0]['status']==4
            denied=(await client.get('/voucher-order/status/'+self.replay_order_id,headers={'authorization':self.tokens[10002]})).json()
            assert not denied['success']
            write(self.path/'cancel-and-ownership.json',{'before':before,'cancel':cancelled,'repeat':repeated,'after':after,'other_user':denied})

    async def replay(self):
        self.stage('real-duplicate-mq-delivery')
        libs=self.path/'temp/java-libs';libs.mkdir()
        with zipfile.ZipFile(self.jar) as archive:
            for member in archive.namelist():
                if member.startswith('BOOT-INF/lib/') and member.endswith('.jar'):
                    (libs/Path(member).name).write_bytes(archive.read(member))
        classpath=str(ROOT/'target/classes')+';'+str(libs/'*')
        self.command(['javac','-encoding','UTF-8','-cp',classpath,'-d',self.path/'temp',HERE/'ReplayOrder.java'])
        message=self.sql(f"SELECT message_body FROM tb_mq_message WHERE order_id={int(self.replay_order_id)} AND topic='seckill-order-topic';")[0][0]
        before=await self.snapshot_voucher(91001)
        result=self.command(['java','-Xmx128m','-XX:MaxDirectMemorySize=64m','-Dfile.encoding=UTF-8',
            '-Duser.home='+str(self.path/'temp/java-home'),'-Drocketmq.client.logRoot='+str(self.path/'temp/mq-client-logs'),'-cp',str(self.path/'temp')+';'+classpath,
            'ReplayOrder',f'127.0.0.1:{self.namesrv_port}','seckill-order-topic',message,self.id],timeout=40)
        (self.path/'mq-replay-producer.log').write_text(result.stdout+result.stderr,encoding='utf-8')
        assert 'VALIDATION_SEND SEND_OK' in result.stdout
        await asyncio.sleep(2)
        after=await self.snapshot_voucher(91001)
        assert after['orders']==before['orders'] and after['mysql_stock']==before['mysql_stock'] and after['redis_stock']==before['redis_stock']
        self.admin(['consumerProgress','-n','namesrv:9876','-g','hmdp-seckill-order-consumer'],'consumer-progress-after-replay')
        write(self.path/'duplicate-mq.json',{'payload':json.loads(message),'before':before,'after':after,
             'sent_ok':True,'note':'Confirm second consumer receipt and zero diff in Java log / consumer progress; DB equality alone is insufficient.'})

    async def agent_tests(self, continuation=False):
        self.stage('real-model-agent-http-java')
        settings=load_settings()
        self.env.update(OPENAI_API_KEY=settings.api_key,OPENAI_BASE_URL=settings.base_url,OPENAI_MODEL=settings.model)
        for i in range(1,4):
            await self.coupon(93000+i,20,shop=9000+i,pay=[6000,5000,3000][i-1],
                rules='以店铺说明为准' if i==3 else '长期有效；全天可用；每桌限1张；2人可用')
        tomorrow=(datetime.now(ZoneInfo('Asia/Shanghai'))+timedelta(days=1)).date().isoformat()
        fixed_cases=[
            {'id':'shop_details','message':'查询店铺9001详情','expected':'shop id9001 and zero-model direct route'},
            {'id':'complex','message':f'帮我推荐附近1公里内适合两个人吃饭、人均100元以内、{tomorrow} 19:00有优惠最划算的三家餐厅。位置120.16,30.25。没有明确使用规则的券不能保证可用。',
             'expected':'people2 budget100 radius1000 exact date; two verified shops and one unverified candidate; verified cash amounts120/130'},
            {'id':'missing','message':'推荐附近两个人吃饭，人均100元，今晚最划算的三家店','expected':'ask location and exact visit_time, zero model'},
            {'id':'resume','message':f'位置120.16,30.25，改成三人、人均80元，{tomorrow} 19:00，1公里内',
             'expected':'reuse task; people3 budget80 radius1000; do not assert two-person voucher eligibility'},
            {'id':'own_order','message':f'订单{self.agent_order}状态','expected':'owner DB status1 maps to HTTP status12; zero model'},
            {'id':'wallet','message':'我的券包','expected':'owner-only wallet; zero model'}]
        write(self.path/'agent_dataset.json',fixed_cases)
        self.sample('before-agent',guard=True)
        log=(self.path/'agent-server.log').open('w',encoding='utf-8');self.files.append(log)
        server=subprocess.Popen([sys.executable,str(HERE/'agent_worker.py'),'server','--run',str(self.path),'--port',str(self.agent_port)],
                                 cwd=ROOT,env=self.env,stdout=log,stderr=subprocess.STDOUT)
        self.processes.append(server)
        async with httpx.AsyncClient(timeout=200,trust_env=False) as client:
            for _ in range(25):
                try:
                    if (await client.get(self.agent_url+'/health')).status_code==200:
                        break
                except Exception:
                    pass
                if server.poll() is not None:
                    raise RuntimeError('Agent HTTP server failed')
                await asyncio.sleep(1)
            else:
                raise RuntimeError('Agent startup timeout')
            async def current(case, body=None):
                ledger_before=load(self.path/'api_usage.json')
                start=time.perf_counter()
                response=await client.post(self.agent_url+'/agent/chat',json=body or {'message':case['message']},
                                          headers={'authorization':self.tokens[self.agent_user]})
                data=response.json()
                elapsed=(time.perf_counter()-start)*1000
                ledger_after=load(self.path/'api_usage.json')
                new_calls=ledger_after['calls'][len(ledger_before['calls']):]
                passed=False
                if response.status_code==200:
                    if case['id']=='shop_details':
                        passed=data.get('data',{}).get('id')==9001 and any(t['type']=='route' and t.get('strategy')=='direct' for t in data['trace']) and not new_calls
                    elif case['id']=='complex':
                        decision=data.get('decision',{})
                        verified=decision.get('recommendations',[])
                        candidates=decision.get('unverified_candidates',[])
                        opts=data.get('constraints',{})
                        passed=(opts.get('people')==2 and opts.get('budget_per_person')==100 and opts.get('radius_m')==1000
                                and {r['shop_id'] for r in verified}=={9001,9002}
                                and {r['shop_id'] for r in candidates}=={9003}
                                and {r['estimated_cash_cost'] for r in verified}=={120,130})
                    elif case['id']=='missing':
                        passed=data.get('state')=='waiting_input' and not new_calls
                    elif case['id']=='resume':
                        passed=(data.get('task_id')==body['task_id'] and data.get('constraints',{}).get('people')==3
                                and data['constraints']['budget_per_person']==80 and not data.get('decision',{}).get('recommendations'))
                    elif case['id']=='own_order':
                        passed=data.get('data',{}).get('statusCode')==12 and not new_calls
                    elif case['id']=='wallet':
                        records=data.get('data',{}).get('records',[])
                        passed=bool(records) and all(r.get('orderId') for r in records) and not new_calls
                model_required=case['id'] in ('complex','resume')
                actual_model_ok=not model_required or (len(new_calls)==2 and all(c['status']==200 and c.get('usage') for c in new_calls)
                    and not any(t.get('type')=='degradation' for t in data.get('trace',[])))
                row={'mode':'router_supervisor_dag','case_id':case['id'],'http_status':response.status_code,'business_passed':passed,
                     'real_model_verified':actual_model_ok,'status':'PASS' if passed and actual_model_ok else 'NOT_VERIFIED' if passed else 'FAIL',
                     'actual_chat_requests':sum(c['kind']=='chat' for c in new_calls),
                     'actual_tokens':sum(c.get('total_tokens') or 0 for c in new_calls),
                     'tool_calls':data.get('metrics',{}).get('tool_calls',0),'latency_ms':elapsed}
                self.agent_rows.append(row)
                write(self.path/('agent-'+case['id']+'.json'),{'case':case,'output':data,'evaluation':row})
                csv_save(self.path/'agent_results.csv',self.agent_rows)
                return data
            # First a bounded comparable task on the exact same isolated business fixture.
            await current(fixed_cases[0])
            if not continuation:
                await self.frozen_comparison(fixed_cases[0])
            await current(fixed_cases[1])
            initial=await current(fixed_cases[2])
            ledger=load(self.path/'api_usage.json')
            chat_left=ledger['limits']['chat_requests']-sum(c['kind']=='chat' for c in ledger['calls'])
            if initial.get('task_id') and chat_left>=2:
                await current(fixed_cases[3],{'message':fixed_cases[3]['message'],'task_id':initial['task_id']})
            elif continuation:
                self.agent_rows.append({'mode':'router_supervisor_dag','case_id':'resume','status':'NOT_VERIFIED',
                    'business_passed':None,'real_model_verified':False,'actual_chat_requests':0,'actual_tokens':0,
                    'reason':'Fewer than two Chat requests remain; did not start a paid task that cannot finish'})
            for case in fixed_cases[4:]:
                await current(case)
        write(self.path/'agent_business_snapshot.json',{
            'shops':self.sql_json('SELECT JSON_OBJECT(\'id\',id,\'avg_price\',avg_price,\'open_hours\',open_hours) FROM tb_shop;'),
            'vouchers':self.sql_json('SELECT JSON_OBJECT(\'id\',v.id,\'shop_id\',shop_id,\'pay_cents\',pay_value,\'face_cents\',actual_value,\'rules\',rules,\'stock\',s.stock) FROM tb_voucher v JOIN tb_seckill_voucher s ON v.id=s.voucher_id WHERE v.id>=93001;'),
            'owner_order':self.sql_json(f"SELECT JSON_OBJECT('id',id,'user_id',user_id,'status',status) FROM tb_voucher_order WHERE id={int(self.agent_order)};")})

    async def frozen_comparison(self, case):
            before=load(self.path/'api_usage.json')
            result=self.command([sys.executable,HERE/'agent_worker.py','baseline','--run',self.path,
                                 '--message',case['message']],timeout=190)
            data=json.loads(result.stdout.strip().splitlines()[-1])
            after=load(self.path/'api_usage.json')
            calls=after['calls'][len(before['calls']):]
            traces=data.get('trace',[])
            passed=any(t.get('tool')=='shop_details' and t.get('result',{}).get('id')==9001 for t in traces)
            row={'mode':'original_sequential_v02','case_id':'shop_details','http_status':data.get('http_status'),
                 'business_passed':passed,'real_model_verified':bool(calls) and not data.get('error'),
                 'status':'PASS' if passed and not data.get('error') else 'NOT_VERIFIED','actual_chat_requests':len(calls),
                 'actual_tokens':sum(c.get('total_tokens') or 0 for c in calls),
                 'tool_calls':sum(t.get('type')=='tool_result' for t in traces),'latency_ms':data.get('duration_ms')}
            self.agent_rows.append(row)
            write(self.path/'agent-frozen-shop_details.json',{'output':data,'evaluation':row})
            csv_save(self.path/'agent_results.csv',self.agent_rows)

    async def semantic_rag(self):
        self.stage('real-semantic-rag')
        self.sample('before-rag',guard=True)
        self.create('pgvector','pgvector/pgvector:pg16',['--memory','384m','--memory-swap','384m',
             '--tmpfs','/var/lib/postgresql/data:rw,size=128m','-p',f'127.0.0.1:{self.pg_port}:5432',
             '-e','POSTGRES_PASSWORD='+self.mysql_password,'-e','POSTGRES_DB=rag_validation'],
             ['postgres','-c','shared_buffers=32MB','-c','max_connections=15'])
        from dataclasses import replace
        from rag.ingestion import chunks
        from rag.lexical import BM25,rrf
        from rag.retrieval import EmbeddingClient,Reranker
        from rag.store import PgVectorStore
        cfg=replace(load_settings(),pgvector_dsn=f'postgresql://postgres:{self.mysql_password}@127.0.0.1:{self.pg_port}/rag_validation')
        store=PgVectorStore(cfg.pgvector_dsn,cfg.embedding_dimensions)
        for _ in range(20):
            try:
                await store.initialize()
                break
            except Exception:
                await asyncio.sleep(1)
        else:
            raise RuntimeError('Dedicated pgvector startup failed')
        document=ROOT/'docs/AGENT_BUSINESS_KNOWLEDGE.md'
        sections=[]
        for part in document.read_text(encoding='utf-8').split('\n## ')[1:]:
            title,text=part.split('\n',1)
            sections.extend(chunks(text, 'repo:docs/AGENT_BUSINESS_KNOWLEDGE.md#'+title,title=title,size=600,overlap=80))
        # Fixed human-reviewed relevance by source section before any retrieval/model call.
        cases=[
            {'id':'cancel','query':'已经付过款的订单，还能像未付款订单一样取消吗？','titles':['取消订单与超时']},
            {'id':'qualification','query':'接口说抢券请求已经收到了，这就代表订单一定生成了吗？','titles':['秒杀购买与使用规则','订单状态与异步查询']},
            {'id':'validity','query':'优惠券开抢到结束的那段时间，是不是也限制我去店里消费的日期？','titles':['秒杀购买与使用规则']},
            {'id':'estimate','query':'两个人的人均参考价乘二，再算上券，等于店里最终账单吗？','titles':['推荐指标与消费估算']},
            {'id':'ambiguous','query':'买了券就肯定能用吗，写着商家说了算还能保证今晚用？','titles':['秒杀购买与使用规则','店铺服务说明']},
            {'id':'privacy','query':'知道别人的订单编号，可以替他付款或者取消吗？','titles':['登录、确认与隐私','取消订单与超时']},
            {'id':'no_answer','query':'2027年春节杭州限行尾号是什么？','titles':[]},
            {'id':'no_answer_menu','query':'验收餐厅9001今天的包间订金具体是多少元？','titles':[]}]
        write(self.path/'rag_dataset.json',{'document_sha256':hashlib.sha256(document.read_bytes()).hexdigest(),
            'annotation':'Source sections manually checked against repository text before calls; no tuning after results',
            'documents':[d.to_dict() for d in sections], 'cases':cases})
        embedding=EmbeddingClient(cfg,RealBudgetTransport(self.path))
        start=time.perf_counter();vectors,doc_usage=await embedding.embed([d.text for d in sections]);doc_ms=(time.perf_counter()-start)*1000
        await store.upsert_document(sections,vectors,cfg.embedding_model)
        start=time.perf_counter();query_vectors,query_usage=await embedding.embed([c['query'] for c in cases]);query_batch_ms=(time.perf_counter()-start)*1000
        for case,vector in zip(cases,query_vectors):
            start=time.perf_counter();sparse=BM25([d.to_dict() for d in sections]).search(case['query'],20);sparse_ms=(time.perf_counter()-start)*1000
            start=time.perf_counter();dense=await store.dense(vector,cfg.embedding_model,20);dense_ms=(time.perf_counter()-start)*1000
            start=time.perf_counter();merged=rrf(sparse,dense);merge_ms=(time.perf_counter()-start)*1000
            insufficient=not sparse and (not dense or dense[0]['distance']>.45)
            hybrid=[] if insufficient else merged[:5]
            before=load(self.path/'api_usage.json')
            start=time.perf_counter()
            ranked,backend,warning=await Reranker(cfg,RealBudgetTransport(self.path)).rank(case['query'],[] if insufficient else merged,5)
            rerank_ms=(time.perf_counter()-start)*1000
            after=load(self.path/'api_usage.json');calls=after['calls'][len(before['calls']):]
            outputs=[('bm25',sparse[:5],sparse_ms,'PASS'),('dense',dense[:5],dense_ms,'PASS'),
                     ('hybrid_rrf',hybrid,sparse_ms+dense_ms+merge_ms,'PASS'),
                     ('rerank',ranked,sparse_ms+dense_ms+merge_ms+rerank_ms,
                      'PASS' if backend=='model_api' or insufficient else 'NOT_VERIFIED')]
            relevant={d.id for d in sections if d.title in case['titles']}
            for mode,hits,elapsed,status in outputs:
                ids=[h['id'] for h in hits]
                rank=next((i for i,identifier in enumerate(ids,1) if identifier in relevant),None)
                citation_checks=[await store.source(identifier) for identifier in ids]
                assert all(row['id']==identifier for row,identifier in zip(citation_checks,ids))
                self.rag_rows.append({'case_id':case['id'],'mode':mode,'status':status,'answerable':bool(relevant),
                    'recall_at_5':len(set(ids)&relevant)/len(relevant) if relevant else None,
                    'mrr':1/rank if rank else 0 if relevant else None,
                    'citation_relevance_precision_at_5':len(set(ids)&relevant)/len(ids) if ids else (1 if not relevant else 0),
                    'citation_source_integrity':1,'no_answer_abstained':not ids if not relevant else None,
                    'retrieval_ms_excluding_embedding':elapsed,'query_embedding_batch_ms':query_batch_ms,
                    'rerank_backend':backend if mode=='rerank' else '',
                    'rerank_requests':len(calls) if mode=='rerank' else 0,'warning':warning if mode=='rerank' else ''})
            write(self.path/f'rag-{case["id"]}.json',{'case':case,'relevant_ids':sorted(relevant),'sparse':sparse,'dense':dense,
                 'hybrid':hybrid,'reranked':ranked,'backend':backend,'warning':warning})
            csv_save(self.path/'rag_comparison.csv',self.rag_rows)
        write(self.path/'rag_embedding_usage.json',{'document_batch_usage':doc_usage,'query_batch_usage':query_usage,
              'document_embedding_ms':doc_ms,'query_embedding_batch_ms':query_batch_ms,
              'note':'Batch query embedding shared across modes; per-query network embedding latency is not measured, do not treat retrieval-only latency as E2E.'})

    def cleanup(self):
        self.stage('safe-cleanup')
        for process in reversed(self.processes):
            if process.poll() is None:
                self.command(['taskkill','/PID',str(process.pid),'/T','/F'],check=False,timeout=15)
                try: process.wait(timeout=10)
                except subprocess.TimeoutExpired: pass
        for stream in self.files:
            stream.close()
        for role,identifier in self.containers.items():
            result=self.command(['docker','logs','--tail','3000',identifier],check=False)
            content=(result.stdout+result.stderr).replace(self.mysql_password,'[redacted-test-password]')
            (self.path/(role+'.log')).write_text(content,encoding='utf-8')
        self.stop_monitor.set()
        if hasattr(self,'monitor_thread'):
            self.monitor_thread.join(timeout=40)
        for mode in ('-DryRun','-Apply'):
            result=self.command(['pwsh','-NoProfile','-File',ROOT/'scripts/test_cleanup.ps1','-RunDirectory',self.path,mode],check=False,timeout=100)
            (self.path/('cleanup'+mode+'.log')).write_text(result.stdout+result.stderr,encoding='utf-8')
            if result.returncode and mode=='-Apply':
                self.failure=self.failure or 'Cleanup retained resources; inspect manifest'
        self.command(['docker','compose','--profile','agent','--profile','dashboard','stop'],check=False,timeout=30)
        self.sample('after-cleanup')
        versions=load(self.path/'versions.json',{})
        volumes=self.command(['docker','volume','ls','--format','{{.Name}}']).stdout.splitlines()
        write(self.path/'final_state.json',{'normal_volumes_before':versions.get('normal_volumes_before'), 'normal_volumes_after':volumes,
            'normal_volumes_preserved':sorted(volumes)==sorted(versions.get('normal_volumes_before',[])),
            'containers':self.command(['docker','ps','-a','--format','{{.Names}} {{.Status}}']).stdout.splitlines(),
            'docker_disk':self.command(['docker','system','df']).stdout.splitlines(),
            'D_free_bytes':shutil.disk_usage('D:/').free})
        # A late PowerShell/Docker helper can recreate TEMP as an empty directory.
        # Remove only the registered empty directory after all subprocesses finish.
        transient=self.path/'temp'
        if transient.exists() and not any(transient.iterdir()):
            transient.rmdir()

    def report(self):
        results={'run_id':self.id,'failure':self.failure,'java':self.java_rows,'agent':self.agent_rows,
            'rag':self.rag_rows,'api_usage':load(self.path/'api_usage.json'),'cleanup':load(self.path/'cleanup_report.json'),
            'resource':load(self.path/'resource_usage.json'),'final_state':load(self.path/'final_state.json')}
        write(self.path/'real_results.json',results)
        def table(rows,fields):
            if not rows:return 'NOT_VERIFIED：阶段未完成，见失败原因与日志。'
            return '\n'.join(['| '+' | '.join(fields)+' |','| '+' | '.join('---' for _ in fields)+' |']+
                ['| '+' | '.join(str(row.get(k,'')) for k in fields)+' |' for row in rows])
        usage=results['api_usage'];summary=[]
        for kind in ('chat','embedding','rerank'):
            calls=[c for c in usage['calls'] if c['kind']==kind]
            summary.append({'kind':kind,'actual_requests':len(calls),'known_tokens':sum(c.get('total_tokens') or 0 for c in calls),
                            'usage_missing_requests':sum(c.get('total_tokens') is None for c in calls),'cost':'UNKNOWN: no invoice/rate'})
        rag_summary=[]
        for mode in ('bm25','dense','hybrid_rrf','rerank'):
            rows=[r for r in self.rag_rows if r['mode']==mode and r['answerable'] and r['status']=='PASS']
            negatives=[r for r in self.rag_rows if r['mode']==mode and not r['answerable'] and r['status']=='PASS']
            if rows:
                rag_summary.append({'mode':mode,'verified_answerable_queries':len(rows),'recall_at_5':sum(r['recall_at_5'] for r in rows)/len(rows),
                    'mrr':sum(r['mrr'] for r in rows)/len(rows),'citation_relevance_precision':sum(r['citation_relevance_precision_at_5'] for r in rows)/len(rows),
                    'no_answer_abstained':str(sum(r['no_answer_abstained'] for r in negatives))+'/'+str(len(negatives))})
        local=[r for r in self.agent_rows if r['mode']=='router_supervisor_dag']
        text=f'''# LocalPerks AI v0.3 真实专项验收

运行：`{self.id}`；证据目录：`{self.path}`。本轮未使用Mock业务、模型响应或合成向量。失败：`{self.failure}`。

## 环境与复现

命令：`pwsh -NoProfile -File scripts/run_real_e2e.ps1`。使用现有镜像和Python虚拟环境，Maven离线构建，源码及Git不提交/推送。
隔离使用本轮标签/独立网络的MySQL dianping_test、Redis、RocketMQ NameServer/Broker、pgvector，数据全部tmpfs，不挂正常卷。业务账号、餐厅、券和订单均为新造测试数据；RAG文档来自仓库真实业务知识文档，按源章节冻结人工相关性标注。
Java实际HTTP服务、FastAPI实际HTTP服务、真实Java工具调用及真实远程模型。冻结原版在独立解释器内ASGI运行，但访问同一真实Java/Redis。延迟口径需区分此差异；原版/新版样本极少，P50/P95不代表生产容量。

配置缺陷修复：Redisson原先固定6379，现改为Spring RedisProperties的地址、端口、数据库、SSL与密码，避免锁与普通缓存落入不同Redis。真实测试中正常6379始终停止。除此之外未重构Agent业务架构。

## 真实Java秒杀与最终一致性

{table(self.java_rows,['concurrency','http_requests','http_accepted','http_429','http_5xx','lua_qualified','final_mysql_orders','initial_stock','mysql_remaining_stock','redis_remaining_stock','outbox_sent_with_message_id','p50_ms','p95_ms','request_rps','status'])}

原有IP限流30次/分钟保留。每档开始精确清理专用Redis中该测试批次创建的限流窗口Key（见limiter-window-resets.jsonl），不提高档内上限；50/100请求可能被429拦截。因此并发用户、HTTP接受、Lua资格和最终订单分别列出，不能把100请求称为100条成单链路。
各档SQL订单、库存、outbox message_id、本人状态接口及HTTP原始响应见java-wave-*.json；消费进度见consumer-progress-*.log。消息发送成功不单独等同消费确认，需结合消费offset差值、Java消费日志及MySQL事实。
同人重复请求、过期/未开始活动回补、MySQL库存不足后的异步失败补偿、重复MQ投递、取消CAS回补和越权读取证据分别保存在对应JSON与生产者/消费者日志。只有存在对应证据时才视为验证，缺失即NOT_VERIFIED。

## 真实Agent与原版对照

{table(self.agent_rows,['mode','case_id','status','business_passed','real_model_verified','actual_chat_requests','actual_tokens','tool_calls','latency_ms'])}

新版任务程序判定通过：{sum(bool(r['business_passed']) for r in local)}/{len(local)}；业务与模型两项均通过：{sum(r['status']=='PASS' for r in local)}/{len(local)}。
新版已执行任务延迟P50={pct([r['latency_ms'] for r in local if r.get('latency_ms') is not None],.5)}ms，P95={pct([r['latency_ms'] for r in local if r.get('latency_ms') is not None],.95)}ms（混合任务，小样本，不是压测）。
快路由允许0模型请求但必须查真实Java；复合任务必须有两次真实模型usage、DAG工具证据和Decimal金额/约束断言，模型降级或预算阻止时标为NOT_VERIFIED，不能算真实模型通过。
本轮原版/新版完整可比集只选店铺详情单样例；复杂原版预计需额外多次调用，未为凑样本突破预算。不能把单例调用差或延迟外推为生产收益。

## 真实语义RAG

{table(rag_summary,['mode','verified_answerable_queries','recall_at_5','mrr','citation_relevance_precision','no_answer_abstained'])}

{table(self.rag_rows,['case_id','mode','status','recall_at_5','mrr','citation_relevance_precision_at_5','no_answer_abstained','retrieval_ms_excluding_embedding','rerank_backend'])}

使用真实Embedding向量和pgvector，真实Rerank接口；Rerank降级的行标记NOT_VERIFIED，不纳入真实重排均值。相关性标注由真实源章节固定生成于请求前，不因负结果改变。引用准确性分别核验源ID存在/内容一致与Top5相关性精度；不是LLM自评。
查询向量采用一次批处理，各策略复用同一向量；表中检索延迟不含这次共享Embedding网络时间，独立耗时与usage在rag_embedding_usage.json。无答案查询单独报告是否拒绝返回证据，不把它们混入Recall/MRR分母。若无答案仍检出文档，保留真实负结果。

## API预算与成本

{table(summary,['kind','actual_requests','known_tokens','usage_missing_requests','cost'])}

累计已知Token：{usage['known_tokens']}；总预算50000，Chat请求最多8，Embedding最多2批，Rerank最多8。Chat预留10000 Token给RAG。关闭原因：`{usage.get('closed')}`。账本先登记实际出站尝试，再记录供应商usage；usage未知时停止所有新增请求。单次预留估算不保证供应商实际用量绝不突破，超出则明确记录并停止。不使用估算Token或公开模型单价冒充本账号真实费用。

## 资源、清理与限制

资源采样与峰值见resource_usage.json/.md；所有档位前检查可用内存、D盘、Docker状态。Java压测与模型评测串行；无本地模型、无新镜像下载。日志、CSV、JSON和必要失败证据保留；正常卷、正常tasks.sqlite3不写。
清理：{json.dumps(results['cleanup'],ensure_ascii=False)}。
最终资源：{json.dumps(results['final_state'],ensure_ascii=False)}。
本轮证据目录写报告前大小：{sum(p.stat().st_size for p in self.path.rglob('*') if p.is_file())} bytes。Windows物理空间与Docker内部回收分别记录，tmpfs释放不等于WSL虚拟磁盘缩小。

## 可用于简历的实测描述

- 在完全独立的MySQL/Redis/RocketMQ环境，分档核验真实HTTP接收、Lua资格、消息发送/消费和最终SQL订单，保留数据库与消费offset证据；具体通过档位以上表为准。
- 通过同用户重复下单、重复MQ投递及库存不足补偿实验验证异步订单链路，避免将HTTP接收冒充最终成单。
- 将真实LLM调用接入持久化预算账本，分别统计Chat/Embedding/Rerank请求与供应商usage，快路由以零模型请求访问真实业务API。
- 在真实仓库知识文档上冻结相关性标注，完成真实语义检索消融并如实记录无答案与重排负结果；未完成项不写成通过。
'''
        (self.path/'REAL_E2E_REPORT.md').write_text(text,encoding='utf-8')
        (ROOT/'docs/REAL_E2E_REPORT.md').write_text(text,encoding='utf-8')
        report=ROOT/'docs/TEST_REPORT.md'
        prior=report.read_text(encoding='utf-8').split('\n## 真实专项验收更新')[0]
        report.write_text(prior+f'\n\n## 真实专项验收更新\n\n本轮 `{self.id}` 的真实Java/Agent/RAG专项结果见 [REAL_E2E_REPORT.md](REAL_E2E_REPORT.md)。失败：`{self.failure}`；真实API已知Token：{usage["known_tokens"]}。上文为上一轮本地验证证据，不冒充本轮真实结果。\n',encoding='utf-8')


async def execute(skip_models=False, continue_from=None):
    run=Run()
    print('RUN_DIRECTORY '+str(run.path),flush=True)
    try:
        if continue_from:
            prior=Path(continue_from).resolve()
            allowed=(ROOT/'artifacts/test-runs').resolve()
            if prior.parent!=allowed or load(prior/'resource_manifest.json')['run_id']!=prior.name:
                raise ValueError('Unproven prior evidence path')
            ledger=load(prior/'api_usage.json')
            previous=load(prior/'real_results.json')
            if ledger['pending'] or ledger['closed'] or not previous['rag']:
                raise RuntimeError('Cannot continue while usage is unknown or RAG is incomplete')
            if any(c.get('total_tokens') is None for c in ledger['calls']):
                raise RuntimeError('Prior API usage is incomplete')
            # A budget may move forward once, never branch into multiple new ledgers.
            claim=prior/'budget_continuation_claim.json'
            with claim.open('x',encoding='utf-8') as stream:
                json.dump({'from_run':prior.name,'to_run':run.id,'source_usage_sha256':hashlib.sha256((prior/'api_usage.json').read_bytes()).hexdigest()},stream,indent=2)
            ledger['chat_soft_token_cap']=50000
            ledger['budget_reallocation']={'from_run':prior.name,'reason':'RAG finished with known usage; release unused RAG reserve, retain same global 8-Chat/50000-token caps'}
            write(run.path/'api_usage.json',ledger)
            run.java_rows=[{**r,'evidence_run':prior.name} for r in previous['java']]
            run.rag_rows=[{**r,'evidence_run':prior.name} for r in previous['rag']]
            run.agent_rows=[{**r,'evidence_run':prior.name} for r in previous['agent'] if r['mode']=='original_sequential_v02']
            write(run.path/'source_run.json',{'run_id':prior.name,'directory':str(prior),'reason':'Reuse verified Java and RAG evidence; no repeated API calls',
                 'api_usage_sha256':hashlib.sha256((prior/'api_usage.json').read_bytes()).hexdigest()})
            csv_save(run.path/'java_performance.csv',run.java_rows)
            csv_save(run.path/'rag_comparison.csv',run.rag_rows)
        run.sample('before')
        run.build()
        await run.infrastructure()
        run.monitor_thread=threading.Thread(target=run.monitor,daemon=True)
        run.monitor_thread.start()
        if continue_from:
            run.stage('continuation-single-order-for-live-agent')
            await run.coupon(94001,2)
            async with httpx.AsyncClient(base_url=run.java_url,timeout=10,trust_env=False) as client:
                response=await client.post('/voucher-order/seckill/94001',headers={'authorization':run.tokens[10001]})
                assert response.json()['success']
                run.agent_order=response.json()['data']['orderId'];run.agent_user=10001
                snapshot=await run.wait_orders(94001,1)
                write(run.path/'continuation_order.json',{'http':response.json(),'final':snapshot})
            await run.agent_tests(continuation=True)
        else:
            await run.java_tests()
        if not skip_models and not continue_from:
            await run.agent_tests()
            try:
                await run.semantic_rag()
            except Exception as exc:
                write(run.path/'rag_not_verified.json',{'status':'NOT_VERIFIED','reason':type(exc).__name__,'message':str(exc)})
                run.failure='RAG phase incomplete: '+type(exc).__name__
        run.sample('end')
    except Exception as exc:
        run.failure=type(exc).__name__+': '+str(exc)
        write(run.path/'failure.json',{'error':type(exc).__name__,'message':str(exc)})
        print('FAILURE '+run.failure,flush=True)
    finally:
        try:
            run.cleanup()
        except Exception as exc:
            write(run.path/'cleanup_failure.json',{'error':type(exc).__name__,'message':str(exc)})
            run.failure=run.failure or 'Cleanup failure'
        run.report()
    print('FINISHED '+str(run.path),flush=True)
    return 1 if run.failure else 0


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--skip-models',action='store_true')
    parser.add_argument('--continue-from')
    args=parser.parse_args()
    raise SystemExit(asyncio.run(execute(args.skip_models,args.continue_from)))
