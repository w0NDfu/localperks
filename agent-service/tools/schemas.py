"""Validate upstream business results without changing the legacy response shape."""
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from tools.java_api import ShopLinkError


class Record(BaseModel):
    model_config = ConfigDict(extra='allow', strict=True)


class ShopRecord(Record):
    id: int = Field(ge=1)
    name: str


class Category(Record):
    id: int = Field(ge=1)
    name: str


class Voucher(Record):
    id: int = Field(ge=1)
    shopId: int = Field(ge=1)
    type: int
    payValue: int | float | None = None
    actualValue: int | float | None = None
    stock: int | None = None


class ShopPage(Record):
    shops: list[ShopRecord]
    page: int


class VoucherPage(Record):
    vouchers: list[Voucher]


class OrderRecord(Record):
    orderId: str = Field(pattern=r'^[0-9]{1,19}$')
    statusCode: int | None = Field(default=None, ge=10, le=18)


class Wallet(Record):
    records: list[OrderRecord]


_SCHEMAS = {name: TypeAdapter(schema) for name, schema in {
    'find_shops': ShopPage, 'shops_by_type': ShopPage, 'shop_details': ShopRecord,
    'shop_types': list[Category], 'find_vouchers': VoucherPage, 'shop_vouchers': VoucherPage,
    'order_status': OrderRecord, 'my_vouchers': Wallet,
}.items()}


def validate_result(name, data):
    schema = _SCHEMAS.get(name)
    if schema:
        try:
            schema.validate_python(data)
        except ValidationError as exc:
            raise ShopLinkError(502, 'SHOPLINK_INVALID_RESPONSE', '业务接口结果不符合工具定义') from exc
    return data
