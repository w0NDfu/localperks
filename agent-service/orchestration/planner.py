from datetime import datetime
import re
from zoneinfo import ZoneInfo

from orchestration.state import ConsumptionConstraints, TaskNode, TaskPlan


class Planner:
    def constraints(self, text, previous=None, supplied=None, timezone="Asia/Shanghai"):
        values = dict(previous or {})
        supplied = supplied or {}
        values.update(supplied)
        if re.search(r'简短|简洁|精简|少说一点',text):
            values['answer_style']='concise'
        elif re.search(r'详细|展开说|多解释',text):
            values['answer_style']='detailed'
        for key,pattern in [('excluded_categories',r'(?:不吃|不要推荐|排除)\s*([\w\u4e00-\u9fff]{1,12})(?=[，。；！?？\s]|$)'),
                            ('cuisine_preferences',r'(?:喜欢|偏好)\s*([\w\u4e00-\u9fff]{1,12})(?=[，。；！?？\s]|$)')]:
            found=re.findall(pattern,text)
            if found:
                values[key]=list(dict.fromkeys(found))
        override=re.findall(r'(?:取消排除|不排除|这次想吃|今天想吃|这次就吃|今天就吃|可以吃)\s*([\w\u4e00-\u9fff]{1,12})(?=[，。；！?？\s]|$)',text)
        if override:
            values['excluded_categories']=[v for v in values.get('excluded_categories',[]) if v not in override]
            values['cuisine_preferences']=list(dict.fromkeys(override))
        if re.search(r'不限制口味|不限分类|清除排除条件',text):
            values['excluded_categories']=[]
        amount = re.search(r"(?:人均|预算|每人|改成|改为)\s*(\d+(?:\.\d+)?)\s*(?:元|块)?", text)
        if amount:
            values["budget_per_person"] = float(amount[1])
        party = re.search(r"([一二两三四五六七八九十\d]+)\s*(?:个)?人", text)
        if party:
            mapping = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
            values["people"] = int(party[1]) if party[1].isdigit() else mapping.get(party[1])
        coords = re.search(r"(?<!\d)(-?\d{1,3}\.\d+)\s*[,，]\s*(-?\d{1,2}\.\d+)(?!\d)", text)
        if coords:
            values["location"] = {"longitude": float(coords[1]), "latitude": float(coords[2])}
        clock = re.search(r"(?:今晚|今天|明天|明晚|晚上|用餐|时间)?\s*([01]?\d|2[0-3])[:：]([0-5]\d)", text)
        explicit_date = re.search(r"\d{4}-\d{2}-\d{2}", text)
        zone = ZoneInfo(timezone)
        today = datetime.now(zone).date()
        if "今晚" in text or "今天" in text:
            values["tonight"] = True
            # Tonight is a window, not a made-up exact dining time.
            if not clock:
                values["visit_time"] = None
        if explicit_date or "明天" in text or "明晚" in text:
            values["tonight"] = False
        if clock:
            date = explicit_date[0] if explicit_date else today.isoformat()
            if "明天" in text or "明晚" in text:
                from datetime import timedelta
                date = (today + timedelta(days=1)).isoformat()
            values["visit_time"] = datetime.fromisoformat(f"{date}T{int(clock[1]):02}:{clock[2]}:00").replace(tzinfo=zone).isoformat()
        if supplied.get("visit_time") and not clock:
            # Explicit structured input takes precedence over ambiguous text.
            values["visit_time"] = supplied["visit_time"]
        top = re.search(r"(?:推荐|最划算的?)\s*([一二两三四五六七八九十\d]+)\s*家", text)
        if top:
            values["top_k"] = int(top[1]) if top[1].isdigit() else {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5}.get(top[1], 3)
        radius = re.search(r"(\d+(?:\.\d+)?)\s*公里", text)
        if radius:
            values["radius_m"] = int(float(radius[1]) * 1000)
        return ConsumptionConstraints.model_validate(values)

    def missing(self, constraints):
        fields = []
        if constraints.location is None:
            fields.append("location")
        if constraints.people is None:
            fields.append("people")
        if constraints.budget_per_person is None:
            fields.append("budget_per_person")
        if constraints.visit_time is None:
            fields.append("visit_time")
        return fields

    def offers(self, shops):
        nodes = [TaskNode(id="shops", agent="shop", operation="candidates", volatile=False)]
        nodes.extend(TaskNode(id=f"voucher_{shop['id']}", agent="voucher", operation="shop_vouchers",
                              dependencies=["shops"], arguments={"shop_id": shop["id"]}, optional=True) for shop in shops)
        nodes.append(TaskNode(id="ranking", agent="decision", operation="rank", optional=True,
                              dependencies=["shops"] + [f"voucher_{shop['id']}" for shop in shops]))
        return TaskPlan(nodes=nodes)
