import hashlib
import json
from dataclasses import dataclass

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

TOOLS = ('spring','events','traffic','mq','inventory','delivery','compensation')


class ObservationScope(BaseModel):
    model_config=ConfigDict(extra='forbid')
    start: int=Field(gt=0)
    end: int=Field(gt=0)
    voucher_id: int=Field(gt=0,le=9223372036854775807)
    ip: str=Field(default='127.0.0.1',min_length=3,max_length=45)

    @model_validator(mode='after')
    def bounded(self):
        import ipaddress
        ipaddress.ip_address(self.ip)
        if not 0<self.end-self.start<=900000:raise ValueError('Window must be within fifteen minutes')
        return self


@dataclass
class LiveProvider:
    client: httpx.AsyncClient
    token: str

    async def read(self, tool, scope):
        if tool not in TOOLS:raise ValueError('Tool not allowed')
        response=await self.client.get('/internal/ops/'+tool, params={'start':scope.start,'end':scope.end,
            'voucherId':str(scope.voucher_id),'ip':scope.ip},headers={'X-Ops-Token':self.token})
        response.raise_for_status()
        result=response.json()
        if result.get('schema')!='ops.v1' or result.get('tool')!=tool or str(result.get('voucherId'))!=str(scope.voucher_id):
            raise ValueError('Observation provenance mismatch')
        if result['windowStart']!=scope.start or result['windowEnd']!=scope.end:raise ValueError('Observation window mismatch')
        return result


def evidence(identifier, tool, raw):
    return {'id':identifier,'tool':tool,'source':'Java /internal/ops/'+tool,'raw':raw,
        'sha256':hashlib.sha256(json.dumps(raw,sort_keys=True,ensure_ascii=False).encode()).hexdigest()}
