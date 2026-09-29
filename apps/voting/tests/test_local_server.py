"""Local launch must not inherit production data or provider credentials."""
from scripts.local_server import local_environment


def test_local_profile_is_isolated_and_preserves_existing_files(tmp_path, monkeypatch):
    existing = b'DATABASE_URL=postgres://production/db\nTELEGRAM_BOT_TOKEN=live-token\n'
    (tmp_path / '.env').write_bytes(existing)
    monkeypatch.setenv('DATABASE_URL', 'postgres://production/db')
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'live-token')
    monkeypatch.setenv('OPENAI_API_KEY', 'live-key')
    monkeypatch.setenv('SECURE_SSL_REDIRECT', 'True')
    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'other.settings')
    env = local_environment(tmp_path)
    assert env['DATABASE_URL'] == 'sqlite:///' + (tmp_path / '.local-voting/db.sqlite3').as_posix()
    assert env['TELEGRAM_BOT_TOKEN'] == env['OPENAI_API_KEY'] == ''
    assert env['DJANGO_DEBUG'] == 'True'
    assert env['SECURE_SSL_REDIRECT'] == 'False'
    assert env['DJANGO_SETTINGS_MODULE'] == 'radio_zimbabwe.settings'
    assert env['DJANGO_ALLOWED_HOSTS'] == '127.0.0.1,localhost'
    assert local_environment(tmp_path)['DJANGO_SECRET_KEY'] == env['DJANGO_SECRET_KEY']
    assert (tmp_path / '.env').read_bytes() == existing


def test_connected_mode_is_opt_in_and_cannot_replace_database(tmp_path):
    (tmp_path / '.env.providers').write_text('TELEGRAM_BOT_TOKEN=test-token\nTELEGRAM_WEBHOOK_SECRET=test-secret\nTELEGRAM_STATION=national_fm\nDATABASE_URL=postgres://wrong/live\nDJANGO_DEBUG=False\n')
    assert local_environment(tmp_path)['TELEGRAM_BOT_TOKEN'] == ''
    connected = local_environment(tmp_path, providers=True)
    assert connected['TELEGRAM_BOT_TOKEN'] == 'test-token'
    assert connected['TELEGRAM_STATION'] == 'national_fm'
    assert connected['DATABASE_URL'].startswith('sqlite:///')
    assert connected['DJANGO_DEBUG'] == 'True'


def test_bird_receive_only_needs_no_channel_or_access_key(tmp_path):
    (tmp_path / '.env.providers').write_text('BIRD_WEBHOOK_SECRET=whsec_dGVzdA==\nBIRD_CHANNEL_ID=\n')
    env = local_environment(tmp_path, providers=True)
    assert env['BIRD_WEBHOOK_SECRET'] == 'whsec_dGVzdA=='
    assert env['BIRD_CHANNEL_ID'] == env['BIRD_ACCESS_KEY'] == ''
    assert local_environment(tmp_path)['BIRD_WEBHOOK_SECRET'] == ''


def test_bird_credentials_without_signing_secret_are_rejected(tmp_path):
    import pytest
    (tmp_path / '.env.providers').write_text('BIRD_ACCESS_KEY=test\nTELEGRAM_BOT_TOKEN=test\nTELEGRAM_WEBHOOK_SECRET=test\n')
    with pytest.raises(RuntimeError, match='BIRD_WEBHOOK_SECRET'):
        local_environment(tmp_path, providers=True)


def test_connected_webhook_hostname_is_explicit_and_isolated(tmp_path):
    (tmp_path / '.env.providers').write_text('BIRD_WEBHOOK_SECRET=test\nWEBHOOK_PUBLIC_HOSTS=station.ngrok-free.dev\n')
    connected = local_environment(tmp_path, providers=True)
    assert connected['DJANGO_ALLOWED_HOSTS'] == '127.0.0.1,localhost,station.ngrok-free.dev'
    assert connected['LOCAL_WEBHOOK_HOSTS'] == 'station.ngrok-free.dev'
    assert local_environment(tmp_path)['LOCAL_WEBHOOK_HOSTS'] == ''


def test_connected_webhook_hostname_rejects_wildcards_and_urls(tmp_path):
    import pytest
    for host in ['*', '.ngrok-free.dev', 'https://station.ngrok-free.dev', 'station.ngrok-free.dev/path', 'localhost', '127.0.0.1', 'station.ngrok-free.dev:8000']:
        (tmp_path / '.env.providers').write_text('BIRD_WEBHOOK_SECRET=test\nWEBHOOK_PUBLIC_HOSTS=' + host + '\n')
        with pytest.raises(RuntimeError, match='exact public hostnames'):
            local_environment(tmp_path, providers=True)
