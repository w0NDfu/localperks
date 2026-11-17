"""Only cached PostgreSQL/Redis images, no business volumes, Java load or real model calls."""
import asyncio
import json
import hashlib
import os
import sys
import time
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/real_e2e'))
from run import Run, free_port, write


async def main():
    run = Run()
    stage = Path(os.environ.get('AGENT_SERVICE_ROOT', ROOT / 'agent-service')).resolve()
    write(run.path / 'api_usage.json', {'actual_requests': 0, 'real_model_calls_permitted': False})
    print('PERSISTENCE_RUN ' + str(run.path), flush=True)
    try:
        run.sample('before', guard=True)
        write(run.path / 'versions.json', {'normal_volumes_before':run.command(['docker','volume','ls','--format','{{.Name}}']).stdout.splitlines(),
            'storage_source_sha256':hashlib.sha256((stage / 'memory/task_store.py').read_bytes()).hexdigest(),
            'model_requests':0, 'purpose':'PostgreSQL/Redis persistence regression'})
        run.network = run.command(['docker','network','create','--label','localperks.validation.run=' + run.id,
            '--label','com.docker.compose.project=' + run.project,run.project]).stdout.strip()
        run.register('network', run.network, 'Dedicated persistence regression network')
        pg_port, redis_port = free_port(), free_port()
        run.create('pgvector', 'pgvector/pgvector:pg16', ['--memory','384m','--memory-swap','384m',
            '--tmpfs','/var/lib/postgresql/data:rw,size=128m','-p',f'127.0.0.1:{pg_port}:5432',
            '-e','POSTGRES_PASSWORD=' + run.mysql_password,'-e','POSTGRES_DB=validation'],
            ['postgres','-c','shared_buffers=32MB','-c','max_connections=40'])
        run.create('redis', 'redis:7.2-alpine', ['--memory','128m','--memory-swap','128m',
            '--tmpfs','/data:rw,size=32m','-p',f'127.0.0.1:{redis_port}:6379'],
            ['redis-server','--save','','--appendonly','no'])
        for _ in range(40):
            ready = run.command(['docker','exec',run.containers['pgvector'],'pg_isready','-U','postgres'], check=False)
            if ready.returncode == 0:
                break
            await asyncio.sleep(.5)
        else:
            raise RuntimeError('Isolated PostgreSQL readiness timeout')
        dsn = f'postgresql://postgres:{run.mysql_password}@127.0.0.1:{pg_port}/validation'
        from cryptography.fernet import Fernet
        env = dict(run.env, AGENT_TEST_STATE_DSN=dsn, AGENT_STATE_DSN=dsn, PGVECTOR_DSN=dsn,
            AGENT_CREDENTIAL_KEY=Fernet.generate_key().decode(),
            REDIS_URL=f'redis://127.0.0.1:{redis_port}/0', AGENT_TEST_REDIS_URL=f'redis://127.0.0.1:{redis_port}/0',
            PYTHONDONTWRITEBYTECODE='1', PYTHONIOENCODING='utf-8',
            PYTHONPATH=os.pathsep.join((str(stage), str(stage / 'tests'), str(ROOT / 'tests'))),
            PERSISTENCE_EVIDENCE=str(run.path), VALIDATION_RUN_DIR=str(run.path))
        env['AGENT_SERVICE_ROOT'] = str(stage)
        targets = ['tests/persistence/test_postgres_state.py', str(stage / 'tests'),
                   'tests/integration', 'tests/reliability', 'tests/security', 'tests/aiops/test_ops_api.py', 'tests/customer_agent']
        if os.environ.get('AGENT_TEST_TARGETS'):
            targets=json.loads(os.environ['AGENT_TEST_TARGETS'])
            if not isinstance(targets,list) or not targets or not all(isinstance(item,str) for item in targets):
                raise ValueError('AGENT_TEST_TARGETS must be a non-empty JSON list of pytest targets')
        # Keep nested cleanup fixtures below Windows' path length limit even
        # when the repository itself is checked out under a deep directory.
        with tempfile.TemporaryDirectory(prefix='lp-test-') as temp_base:
            temp_path = Path(temp_base).resolve()
            assert temp_path.parent == Path(tempfile.gettempdir()).resolve()
            assert temp_path.name.startswith('lp-test-')
            result = run.command([sys.executable,'-m','pytest','-p','pg_state_plugin',*targets,'-q',
                '--basetemp=' + str(temp_path/'pytest'),
                '--junitxml=' + str(run.path/'persistence.xml'),'-o','cache_dir=' + str(run.path/'temp/pytest-cache')],
                env=env, timeout=240, check=False)
        (run.path/'pytest.log').write_text(result.stdout+result.stderr, encoding='utf-8')
        write(run.path/'persistence-result.json', {'test_exit':result.returncode,'model_calls':0})
        print(result.stdout[-2500:], flush=True)
        if result.returncode:
            raise RuntimeError('PostgreSQL regression failed; raw pytest.log retained')
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
