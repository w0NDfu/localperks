import math
import re
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from orchestration.state import ConsumptionConstraints


def money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def distance_m(location, shop):
    try:
        lon, lat = float(shop["x"]), float(shop["y"])
        if not (-180 <= lon <= 180 and -85.05112878 <= lat <= 85.05112878):
            return None
        a1, a2 = math.radians(location.latitude), math.radians(lat)
        da, dl = a2 - a1, math.radians(lon - location.longitude)
        hav = math.sin(da / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dl / 2) ** 2
        return round(6371000 * 2 * math.asin(min(1, math.sqrt(hav))), 1)
    except (KeyError, TypeError, ValueError):
        return None


def parse_time(value, zone):
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=zone) if parsed.tzinfo is None else parsed.astimezone(zone)


def redemption_rule(rules: str, visit: datetime, people: int, purchase_time: datetime):
    """Fail closed: acquisition dates are never treated as redemption dates."""
    if not rules or re.search(r"以.*(店铺|商家|门店).*为准|详询|另行|预约|节假日|周末|不可用|不适用|除外|限.*(时段|日期)", rules):
        return "unknown", "使用条件或特殊日期限制需向商家确认"
    party = re.search(r"([1-9]\d*)人(?:套餐|可用|使用)", rules)
    if party and int(party[1]) != people:
        return "rejected", "券适用人数与本次人数不符"
    dates = re.search(r"(?:使用有效期|核销有效期)[:：]?\s*(\d{4}-\d{2}-\d{2})\s*(?:至|到|~)\s*(\d{4}-\d{2}-\d{2})", rules)
    if dates:
        if not dates[1] <= visit.date().isoformat() <= dates[2]:
            return "rejected", "用餐日期不在规则注明的使用有效期内"
    elif not re.search(r"长期有效|当日(?:可用|有效)|购买当天可用", rules):
        return "unknown", "缺少明确的使用有效期"
    elif '长期有效' not in rules and visit.date() != purchase_time.date():
        return "rejected", "仅购买当日可用，与计划用餐日期不符"
    if not re.search(r"全天可用|每日可用|不限时段", rules):
        return "unknown", "缺少明确的可用时段"
    if not re.search(r"每桌限[一1]张|每单限[一1]张|可使用[一1]张", rules):
        return "unknown", "券叠加/使用数量条件未明确"
    remainder = rules
    for pattern in [r'(?:使用有效期|核销有效期)[:：]?\s*\d{4}-\d{2}-\d{2}\s*(?:至|到|~)\s*\d{4}-\d{2}-\d{2}',
                    r'长期有效|当日可用|当日有效|购买当天可用|全天可用|每日可用|不限时段',
                    r'每桌限[一1]张|每单限[一1]张|可使用[一1]张|[1-9]\d*人(?:套餐|可用|使用)',
                    r'到店使用|不与其他优惠同享|不可与其他优惠同享']:
        remainder = re.sub(pattern, '', remainder)
    if re.search(r'[\u4e00-\u9fffA-Za-z0-9]', remainder):
        return 'unknown', '包含尚未实现确定性校验的使用条件，需商家确认'
    return "verified", "按文本明确的使用日期、时段、人数与单张规则校验"


def shop_open(shop, visit):
    hours = str(shop.get("openHours", ""))
    if "24小时" in hours:
        return True
    ranges = re.findall(r"(\d{1,2}):([0-5]\d)\s*[-–~至]\s*(\d{1,2}):([0-5]\d)", hours)
    if not ranges:
        return None
    minutes = visit.hour * 60 + visit.minute
    for h1, m1, h2, m2 in ranges:
        start, end = int(h1) * 60 + int(m1), int(h2) * 60 + int(m2)
        if end <= start:
            if minutes >= start or minutes < end:
                return True
        elif start <= minutes < end:
            return True
    return False


class DecisionEngine:
    def rank(self, shops: list[dict], offers: dict[int, list[dict]], constraints: ConsumptionConstraints,
             timezone="Asia/Shanghai", now=None):
        zone = ZoneInfo(timezone)
        now = now or datetime.now(zone)
        visit = parse_time(constraints.visit_time, zone)
        verified, candidates, rejected = [], [], []
        for shop in shops:
            shop_id = shop["id"]
            classification=str(shop.get('category_name') or shop.get('typeName') or '').casefold()
            label=(classification+' '+str(shop.get('name',''))).casefold()
            if any(term.casefold() in label for term in constraints.excluded_categories):
                rejected.append({'shop_id':shop_id,'reason':'命中用户明确排除的分类'})
                continue
            matched=[term for term in constraints.cuisine_preferences if term.casefold() in label]
            category_uncertain=bool(constraints.excluded_categories) and classification in ('','美食','餐厅','food','restaurant','restaurants')
            dist = distance_m(constraints.location, shop)
            average = shop.get("avgPrice")
            if dist is None or dist > constraints.radius_m:
                rejected.append({"shop_id": shop_id, "reason": "距离未知或超出半径"})
                continue
            if not isinstance(average, (int, float)) or average <= 0 or money(average) > money(constraints.budget_per_person):
                rejected.append({"shop_id": shop_id, "reason": "人均参考价未知或超出预算"})
                continue
            opened = shop_open(shop, visit)
            if opened is False:
                rejected.append({"shop_id": shop_id, "reason": "计划时刻不在营业时间"})
                continue
            for voucher in offers.get(shop_id, []):
                # Ordinary legacy coupons have no live purchase stock/status contract.
                if voucher.get("type") != 1 or not isinstance(voucher.get("stock"), int) or voucher["stock"] <= 0:
                    rejected.append({"shop_id": shop_id, "voucher_id": voucher.get("id"), "reason": "非秒杀券或实时库存不足/未知"})
                    continue
                try:
                    if not parse_time(voucher["beginTime"], zone) <= now < parse_time(voucher["endTime"], zone):
                        continue
                    pay, face = money(voucher["payValue"]) / 100, money(voucher["actualValue"]) / 100
                    if not 0 <= pay < face:
                        continue
                except (KeyError, TypeError, ValueError, ArithmeticError):
                    continue
                eligibility, reason = redemption_rule(str(voucher.get("rules", "")), visit, constraints.people, now)
                if eligibility == "rejected":
                    rejected.append({"shop_id": shop_id, "voucher_id": voucher.get("id"), "reason": reason})
                    continue
                estimate = money(average) * constraints.people
                estimated_cash = max(estimate - face, Decimal(0)) + pay
                if estimated_cash > money(constraints.budget_per_person) * constraints.people:
                    rejected.append({'shop_id': shop_id, 'voucher_id': voucher['id'], 'reason': '券购买支出超出总预算'})
                    continue
                saving = face - pay
                # A reproducible score, not a probability or a guarantee of actual dining cost.
                discount_ratio = saving / face
                score = money(Decimal("70") * discount_ratio + Decimal("20") * (1 - Decimal(str(dist / constraints.radius_m))) + Decimal("10") * (1 - money(average) / money(constraints.budget_per_person)))
                if matched:
                    score += Decimal('15')
                if category_uncertain:
                    eligibility,reason='unknown','分类信息不足，尚不能确认已排除用户指定的餐饮类别'
                if opened is None:
                    eligibility, reason = "unknown", "营业时间格式不能验证，需联系商家"
                row = {"shop_id": shop_id, "shop_name": shop["name"], "voucher_id": voucher["id"],
                       "distance_m": dist, "average_price": float(money(average)), "people": constraints.people,
                       "voucher_price": float(pay), "face_value": float(face), "face_saving": float(saving),
                       "face_discount_percent": float(money(discount_ratio * 100)),
                       "estimated_group_spend": float(estimate), "estimated_cash_cost": float(money(estimated_cash)),
                       "estimated_per_person": float(money(estimated_cash / constraints.people)),
                       "stock": voucher["stock"], "rules": voucher.get("rules"), "eligibility": eligibility,
                       "eligibility_reason": reason, "score": float(score), "queried_at": now.isoformat(),
                       "preference_matches":matched,
                       "source": f"/voucher/list/{shop_id}", "party_suitability": "按人数估算；座位/包间/接待能力未获接口验证"}
                (verified if eligibility == "verified" else candidates).append(row)
        def unique_top(rows):
            picked = {}
            for row in sorted(rows, key=lambda item: (-item["score"], -item["face_saving"], item["distance_m"], item["voucher_id"])):
                picked.setdefault(row["shop_id"], row)
            return list(picked.values())[:constraints.top_k]
        return {"recommendations": unique_top(verified), "unverified_candidates": unique_top(candidates),
                "rejected": rejected, "constraints": constraints.model_dump(),
                "calculation": "券面节省=面值-券售价；估算实付=max(店铺人均参考价×人数-面值,0)+券售价；每次仅一张，不叠加",
                "score_formula": "70×券面节省比例+20×(1-距离/半径)+10×(1-人均参考价/预算)+15×明确偏好匹配",
                "limitations": ["人均和用餐实付为估算，并非菜单报价", "口味匹配仅依据商家名称和分类，无法验证菜品或过敏原", "券面节省以足额使用面值为前提", "购买时间不等于使用有效期", "库存为查询快照，最终资格和成交以Java校验为准"]}
