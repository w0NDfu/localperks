"""Run the real cleanup script in a disposable miniature workspace."""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def workspace(tmp_path, resources=None):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    script = scripts / 'test_cleanup.ps1'
    shutil.copy2(ROOT / 'scripts/test_cleanup.ps1', script)
    run_id = 'v03-20261105-000000-aabbccdd'
    run = tmp_path / 'artifacts/test-runs' / run_id
    (run / 'temp').mkdir(parents=True)
    (run / '.run-owner').write_text(run_id)
    (run / 'temp/cache.txt').write_text('disposable')
    (run / 'evidence.json').write_text('retained')
    manifest = {'run_id': run_id, 'resources': resources or [
        {'run_id': run_id, 'type': 'directory', 'identifier': str(run / 'temp'), 'auto_cleanup': True,
         'isolated': True, 'cleanup_status': 'pending'}]}
    (run / 'resource_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    return script, run, manifest


def execute(script, run, *args):
    return subprocess.run(['pwsh', '-NoProfile', '-File', str(script), '-RunDirectory', str(run), *args],
                          capture_output=True, timeout=20)


def test_cleanup_defaults_to_dry_run_then_apply_preserves_evidence(tmp_path):
    script, run, _ = workspace(tmp_path)
    assert execute(script, run).returncode == 0
    assert (run / 'temp/cache.txt').exists()
    dry = json.loads((run / 'cleanup_dry_run.json').read_text(encoding='utf-8-sig'))
    assert dry['mode'] == 'DryRun' and dry['actions'][0]['status'] == 'would_remove'
    assert execute(script, run, '-Apply').returncode == 0
    assert not (run / 'temp').exists() and (run / 'evidence.json').exists()
    assert execute(script, run, '-Apply').returncode == 0  # repeat cleanup is safe


@pytest.mark.parametrize('attack', ['outside', 'evidence', 'owner', 'run_id', 'link'])
def test_cleanup_rejects_unproven_targets(tmp_path, attack):
    script, run, manifest = workspace(tmp_path)
    if attack == 'outside':
        manifest['resources'][0]['identifier'] = str(tmp_path)
    elif attack == 'evidence':
        manifest['resources'][0]['identifier'] = str(run / 'evidence.json')
    elif attack == 'owner':
        (run / '.run-owner').write_text('another-run')
    elif attack == 'run_id':
        manifest['resources'][0]['run_id'] = 'another-run'
    else:
        destination = tmp_path / 'business'
        destination.mkdir()
        (destination / 'keep.txt').write_text('business')
        # Directory junction creation does not need symlink privileges on Windows.
        link_path = str(run / 'temp/link').replace("'", "''")
        target_path = str(destination).replace("'", "''")
        result = subprocess.run(['pwsh', '-NoProfile', '-Command',
            f"New-Item -ItemType Junction -Path '{link_path}' -Target '{target_path}' | Out-Null"],
            capture_output=True, timeout=10)
        if result.returncode:
            pytest.skip('Directory junction unavailable')
    (run / 'resource_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    try:
        result = execute(script, run, '-Apply')
        assert result.returncode != 0
        assert (run / 'temp/cache.txt').exists() and (run / 'evidence.json').exists()
    finally:
        if attack == 'link':
            # Remove only this test-created junction itself, never its target.
            (run / 'temp/link').rmdir()
