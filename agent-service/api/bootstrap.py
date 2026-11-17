"""Create only missing local infrastructure settings; never rewrite real credentials."""
import os
import secrets
from pathlib import Path
from urllib.parse import quote
from cryptography.fernet import Fernet


def configure(root=None):
    root = Path(root) if root else Path(__file__).resolve().parents[1]
    path = root / '.env.platform'
    password = secrets.token_urlsafe(32)
    content = ('POSTGRES_PASSWORD=' + password + '\nPGVECTOR_DSN=postgresql://agent:' +
               quote(password, safe='') + '@127.0.0.1:5433/agent_knowledge\nAGENT_CREDENTIAL_KEY=' + Fernet.generate_key().decode() + '\n')
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        print('Existing .env.platform preserved; .env unchanged')
        return False
    with os.fdopen(descriptor, 'w', encoding='utf-8') as file:
        file.write(content)
    print('Created .env.platform; credentials are not printed; .env unchanged')
    return True


if __name__ == '__main__':
    configure()
    # Existing installations explicitly run this command to append the new setting.
    from dotenv import dotenv_values
    target=Path(__file__).resolve().parents[1]/'.env.platform'
    if not dotenv_values(target).get('AGENT_CREDENTIAL_KEY'):
        with target.open('a',encoding='utf-8') as stream:
            stream.write('\nAGENT_CREDENTIAL_KEY='+Fernet.generate_key().decode()+'\n')
        print('Added shared credential encryption key; existing settings preserved')
