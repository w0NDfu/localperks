"""Authenticated, short-lived credentials in Redis, never in the task journal."""
import json
import uuid

from cryptography.fernet import Fernet, InvalidToken

from core.state import AgentError


class CredentialVault:
    def __init__(self, redis, key, namespace, ttl=900):
        if not key:
            raise ValueError('Configure the same AGENT_CREDENTIAL_KEY on every Agent instance')
        self.cipher = Fernet(key.encode())
        self.redis, self.namespace, self.ttl = redis, namespace, ttl

    def key(self, reference):
        return f'agent:credentials:{self.namespace}:{reference}'

    async def put(self, scope, authorization):
        reference = str(uuid.uuid4())
        encrypted = self.cipher.encrypt(json.dumps({'scope': scope, 'authorization': authorization}).encode())
        await self.redis.set(self.key(reference), encrypted, ex=self.ttl)
        return reference

    async def get(self, scope, reference):
        encrypted = await self.redis.get(self.key(reference))
        try:
            if not encrypted:
                raise InvalidToken()
            value = json.loads(self.cipher.decrypt(encrypted.encode() if isinstance(encrypted, str) else encrypted, ttl=self.ttl))
            if value['scope'] != scope:
                raise InvalidToken()
            return value['authorization']
        except (InvalidToken, ValueError, KeyError, TypeError) as exc:
            raise AgentError('REAUTH_REQUIRED', '短期身份凭据已过期，请登录后重交原请求编号', 401) from exc

    async def delete(self, reference):
        await self.redis.delete(self.key(reference))
