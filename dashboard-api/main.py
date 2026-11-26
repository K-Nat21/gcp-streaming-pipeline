import time
from datetime import datetime, timedelta, timezone

import functions_framework
from google.cloud import bigquery

client = bigquery.Client()
TABLE = f"{client.project}.streaming.order_events"
IST = timezone(timedelta(hours=5, minutes=30))

# One query, one table scan. BigQuery bills a 10 MB minimum per query, so the
# result is cached to keep the free tier safe however many people open the page.
CACHE_SECONDS = 20
_cache = {"at": 0.0, "data": None}

SQL = f"""
SELECT
  city,
  product,
  TIMESTAMP_TRUNC(event_time, MINUTE) AS minute,
  COUNT(*) AS orders,
  SUM(amount) AS revenue
FROM `{TABLE}`
GROUP BY GROUPING SETS ((city), (product), (minute))
"""

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def build_payload():
    cities, products, per_minute = [], [], {}
    for row in client.query(SQL).result():
        if row.city is not None:
            cities.append([row.city, float(row.revenue or 0)])
        elif row.product is not None:
            products.append([row.product, int(row.orders)])
        elif row.minute is not None:
            per_minute[row.minute.replace(second=0, microsecond=0)] = int(row.orders)

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    minutes = []
    for i in range(59, -1, -1):
        slot = now - timedelta(minutes=i)
        minutes.append([slot.astimezone(IST).strftime("%H:%M"), per_minute.get(slot, 0)])

    cities.sort(key=lambda r: -r[1])
    products.sort(key=lambda r: -r[1])
    return {
        "cities": cities,
        "products": products,
        "minutes": minutes,
        "updated": datetime.now(IST).strftime("%H:%M:%S"),
    }


@functions_framework.http
def api(request):
    if request.method == "OPTIONS":
        return ("", 204, CORS)
    if _cache["data"] is None or time.time() - _cache["at"] > CACHE_SECONDS:
        _cache["data"] = build_payload()
        _cache["at"] = time.time()
    return (_cache["data"], 200, CORS)
