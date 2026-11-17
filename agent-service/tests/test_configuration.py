def test_bootstrap_creates_only_missing_infrastructure_config_and_never_rewrites_credentials(tmp_path):
    from api.bootstrap import configure
    env = tmp_path / '.env'
    env.write_text('OPENAI_API_KEY=existing-local-secret\n', encoding='utf-8')
    assert configure(tmp_path)
    created = (tmp_path / '.env.platform').read_bytes()
    assert not configure(tmp_path)
    assert (tmp_path / '.env.platform').read_bytes() == created
    assert env.read_text(encoding='utf-8') == 'OPENAI_API_KEY=existing-local-secret\n'
