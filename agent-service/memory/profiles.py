"""Explicit user-owned preferences with revision checks and no inferred writes."""
from typing import Literal
import contextvars
from contextlib import contextmanager

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.state import AgentError
from memory.task_store import DB_NOW, dump

ANSWER_STYLE = contextvars.ContextVar('answer_style',default='neutral')


@contextmanager
def answer_style(value):
    token=ANSWER_STYLE.set(value)
    try:
        yield
    finally:
        ANSWER_STYLE.reset(token)


def style_instruction():
    return {'concise':'用简洁的中文，最多两个段落，保留全部必需事实。',
            'detailed':'用详细的中文解释比较理由与不确定性，不添加事实之外的断言。',
            'neutral':'使用自然、清晰的中文回答。'}[ANSWER_STYLE.get()]


class UserPreferences(BaseModel):
    model_config = ConfigDict(extra='forbid')
    budget_per_person: float | None = Field(default=None,gt=0,le=100000)
    radius_m: int | None = Field(default=None,ge=100,le=5000)
    cuisine_preferences: list[str] = Field(default_factory=list,max_length=8)
    excluded_categories: list[str] = Field(default_factory=list,max_length=8)
    answer_style: Literal['concise','detailed','neutral'] = 'neutral'

    @field_validator('cuisine_preferences','excluded_categories')
    @classmethod
    def clean_labels(cls, values):
        import re
        if any(not re.fullmatch(r'[\w\u4e00-\u9fff -]{1,24}',v) for v in values):
            raise ValueError('偏好只能填写简短的口味或分类名称')
        if any(not v.strip() for v in values):
            raise ValueError('偏好名称不能为空')
        return list(dict.fromkeys(v.strip() for v in values))


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    revision: int = Field(ge=0)
    preferences: UserPreferences


class ProfileStore:
    def __init__(self, store):
        self.store = store

    def authorize(self, scope):
        if not scope.startswith('user:'):
            raise AgentError('LOGIN_REQUIRED','请先登录再管理个人偏好',401)

    async def get(self, scope):
        import json
        self.authorize(scope)
        row = await self.store.call(lambda c:c.execute('SELECT preferences,revision,updated,source FROM agent_profiles WHERE scope=%s',(scope,)).fetchone())
        return {**row,'preferences':json.loads(row['preferences'])} if row else {'preferences':{},'revision':0,'updated':None,'source':'user'}

    async def put(self, scope, preferences, revision):
        self.authorize(scope)
        normalized = UserPreferences.model_validate(preferences).model_dump(exclude_none=True)
        def update(c):
            c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('profile:'+scope,))
            row=c.execute('SELECT revision FROM agent_profiles WHERE scope=%s',(scope,)).fetchone()
            if (row['revision'] if row else 0)!=revision:
                raise AgentError('PROFILE_CONFLICT','偏好已在其他页面更新，请刷新后再保存',409)
            c.execute(f"INSERT INTO agent_profiles(scope,preferences,revision,source,updated) VALUES(%s,%s,%s,'user',{DB_NOW}) ON CONFLICT(scope) DO UPDATE SET preferences=excluded.preferences,revision=excluded.revision,source='user',updated=excluded.updated",
                      (scope,dump(normalized),revision+1))
        await self.store.call(update)
        return await self.get(scope)

    async def delete(self, scope, revision):
        # Retain only the revision to prevent a stale browser from restoring deleted values.
        value = await self.put(scope,{},revision)
        return value
