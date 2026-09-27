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
