import base64, json
import functions_framework
from google.cloud import bigquery

client = bigquery.Client()
TABLE = f"{client.project}.streaming.order_events"

@functions_framework.cloud_event
def handle_event(cloud_event):
    raw = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    event = json.loads(raw)

    # basic validation: skip bad records instead of crashing
    if not event.get("order_id") or event.get("amount", 0) <= 0:
        print(f"Invalid event skipped: {event}")
        return

    # row_ids lets BigQuery dedupe retries (Pub/Sub is at-least-once)
    errors = client.insert_rows_json(TABLE, [event], row_ids=[event["order_id"]])
    if errors:
        raise RuntimeError(errors)  # raising makes Pub/Sub retry
