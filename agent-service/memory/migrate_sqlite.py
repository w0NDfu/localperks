"""Read-only legacy import. Dry run by default; never enables a SQLite runtime backend."""
import argparse
import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from psycopg import sql
from core.config import load_settings
from memory.task_store import TaskStore, DB_NOW

KEYS = {'agent_tasks': ('id',), 'agent_events': ('seq',), 'agent_checkpoints': ('task_id', 'node_id'),
        'agent_actions': ('id',), 'agent_action_events': ('seq',), 'ops_model_usage': ('id',)}


def snapshot(source):
    source = Path(source).resolve(strict=True)
    if source.suffix not in {'.sqlite3', '.sqlite', '.db'}:
        raise ValueError('Select an explicit legacy SQLite database')
    if Path(str(source) + '-wal').exists() and Path(str(source) + '-wal').stat().st_size:
        raise ValueError('Source still has WAL data: stop the old writer and supply a consistent offline SQLite backup')
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    tables = {}
    # WAL is explicitly rejected above. Immutable mode avoids creating SHM/WAL sidecars
    # beside the stopped, source-only archive (including on Windows).
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        available = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table, keys in KEYS.items():
            if table in available:
                # Identifiers come only from the fixed table/key allowlist above.
                tables[table] = [dict(row) for row in conn.execute('SELECT * FROM ' + table + ' ORDER BY ' + ','.join(keys))]
    if hashlib.sha256(source.read_bytes()).hexdigest() != before:
        raise ValueError('Source changed during inspection; stop the writer')
    if Path(str(source) + '-wal').exists() and Path(str(source) + '-wal').stat().st_size:
        raise ValueError('A writer started during inspection')
    canonical = json.dumps(tables, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return tables, {'source_sha256': before, 'snapshot_sha256': hashlib.sha256(canonical.encode()).hexdigest(),
                    'counts': {table: len(rows) for table, rows in tables.items()}}


def migrate(source, store, *, acknowledge_writers_stopped=False):
    if not acknowledge_writers_stopped:
        raise ValueError('Stop source and target writers before applying the one-time import')
    tables, manifest = snapshot(source)
    def import_all(conn):
        conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('localperks-state-import:' + store.schema,))
        previous = conn.execute('SELECT counts FROM agent_imports WHERE source_sha256=%s', (manifest['snapshot_sha256'],)).fetchone()
        if previous:
            return {**manifest, 'status': 'already_imported', 'inserted': 0}
        conn.execute(sql.SQL('LOCK TABLE {} IN EXCLUSIVE MODE').format(sql.SQL(',').join(sql.Identifier(t) for t in KEYS)))
        inserted, verified = 0, {}
        for table, rows in tables.items():
            target_columns = {r['column_name'] for r in conn.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s', (store.schema, table))}
            if rows and not set(rows[0]) <= target_columns:
                raise ValueError('Unexpected source columns: ' + table)
            for original in rows:
                row = dict(original)
                # A stopped task can be resumed; ambiguous transaction actions remain running/unknown.
                if table == 'agent_tasks' and row['state'] == 'running':
                    row['lease_until'] = 0
                columns, keys = list(row), KEYS[table]
                where = sql.SQL(' AND ').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in keys)
                existing = conn.execute(sql.SQL('SELECT * FROM {} WHERE {}').format(sql.Identifier(table), where), tuple(row[k] for k in keys)).fetchone()
                if existing:
                    if any(existing[k] != value for k, value in row.items()):
                        raise ValueError('Conflicting target record in ' + table + '; whole import rolled back')
                else:
                    conn.execute(sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(sql.Identifier(table),
                        sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(sql.Placeholder() for _ in columns)), tuple(row.values()))
                    inserted += 1
                saved = conn.execute(sql.SQL('SELECT * FROM {} WHERE {}').format(sql.Identifier(table), where), tuple(row[k] for k in keys)).fetchone()
                if any(saved[k] != value for k, value in row.items()):
                    raise ValueError('Readback mismatch in ' + table)
            verified[table] = len(rows)
        for table, column in (('agent_events', 'seq'), ('agent_action_events', 'seq'), ('ops_model_usage', 'id')):
            sequence = conn.execute('SELECT pg_get_serial_sequence(%s,%s) AS name', (store.schema + '.' + table, column)).fetchone()['name']
            maximum = conn.execute(sql.SQL('SELECT COALESCE(MAX({}),0) AS n FROM {}').format(sql.Identifier(column), sql.Identifier(table))).fetchone()['n']
            conn.execute('SELECT setval(%s::regclass,%s,false)', (sequence, maximum + 1))
        conn.execute(f'INSERT INTO agent_imports(source_sha256,imported,counts) VALUES(%s,{DB_NOW},%s)',
                     (manifest['snapshot_sha256'], json.dumps(manifest['counts'])))
        return {**manifest, 'status': 'imported', 'inserted': inserted, 'verified_rows': verified}
    result = store.call_sync(import_all)
    if hashlib.sha256(Path(source).read_bytes()).hexdigest() != manifest['source_sha256']:
        raise ValueError('Source changed; inspect import before enabling either writer')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--acknowledge-writers-stopped', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.apply:
        settings = load_settings()
        store = TaskStore(settings.state_dsn, schema=settings.state_schema)
        try:
            result = migrate(args.source, store, acknowledge_writers_stopped=args.acknowledge_writers_stopped)
        finally:
            store.close()
    else:
        _, result = snapshot(args.source)
        result['status'] = 'dry_run'
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
