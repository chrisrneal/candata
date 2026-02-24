---
title: candata — Canadian Data Explorer
---

# candata Public Dashboard

Free access to Canadian economic, housing, and procurement data.

```sql provinces
select
  g.name,
  g.sgc_code,
  count(distinct iv.indicator_id) as indicators_available
from geographies g
left join indicator_values iv on iv.geography_id = g.id
where g.level = 'pr'
group by g.name, g.sgc_code
order by g.name
```

<DataTable data={provinces} />

---

*Data sourced from Statistics Canada, Bank of Canada, and CMHC. Updated regularly by the candata pipeline.*
