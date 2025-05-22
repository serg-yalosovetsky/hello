from prefect import flow, task
import json
import mysql.connector
import paramiko
from azure.data.tables import TableServiceClient
from datetime import datetime
from pathlib import Path
from io import BytesIO, TextIOWrapper
import csv


@task
def read_db_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

@task
def extract_value(value):
    return value.value if hasattr(value, 'value') else value

@task
def is_meaningful_row(row_data, exclude_fields=None):
    exclude_fields = exclude_fields or {"PartitionKey", "RowKey"}
    return any(
        value not in ("", None)
        for key, value in row_data.items()
        if key not in exclude_fields
    )

@task
def ensure_remote_dir(sftp, remote_directory):
    dirs = remote_directory.strip('/').split('/')
    current_path = ''
    for dir_part in dirs:
        current_path += '/' + dir_part
        try:
            sftp.chdir(current_path)
        except IOError:
            sftp.mkdir(current_path)
            print(f"Created remote directory: {current_path}")
            sftp.chdir(current_path)

@task
def insert_sync_event(conn, event_data):
    cursor = conn.cursor()
    cursor.execute("SELECT SF_SyncEventInsert(%s) AS result_json", (json.dumps(event_data),))
    row = cursor.fetchone()
    conn.commit()
    cursor.close()

    result = json.loads(row[0])
    event_data["id"] = result.get("id", result)
    return event_data

@task
def update_sync_event(conn, event, success, log_message):
    def clean(value):
        if isinstance(value, str) and value.strip().lower() == "null":
            return None
        return value if value not in ("", None) else None

    required_keys = ["id", "scheduleId", "tableName", "tableId", "fileName"]
    missing = [k for k in required_keys if k not in event]
    if missing:
        raise KeyError(f"Missing keys in sync event: {missing}\nEvent: {json.dumps(event, indent=2)}")

    payload = {
        "id": event["id"],
        "scheduleId": event["scheduleId"],
        "tableName": event["tableName"],
        "tableId": event["tableId"],
        "filename": event["fileName"],
        "rowCount": event.get("rowCount"),
        "lastRowId": clean(event.get("lastRowId")),
        "lastRowTimestamp": clean(event.get("lastRowTimestamp")),
        "finishedAt": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "success": int(success),
        "log": log_message
    }

    payload_cleaned = {k: v for k, v in payload.items() if v is not None}
    cursor = conn.cursor()
    cursor.execute("SELECT SF_SyncEventUpdate(%s)", (json.dumps(payload_cleaned),))
    cursor.fetchone()
    conn.commit()
    cursor.close()


@flow
def main():
    schedule_id = 1

    db_config = read_db_config()
    conn = mysql.connector.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )

    cursor = conn.cursor()
    cursor.execute("SELECT SF_ScheduleDetails(%s) AS result_json", (schedule_id,))
    result_json = json.loads(cursor.fetchone()[0])
    cursor.close()

    sync = result_json["synchronization"]
    azure = sync["azure_storage"]
    ftp_server = sync["ftp_server"]

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=ftp_server["host"],
        port=ftp_server["port"],
        username=ftp_server["user"],
        password=ftp_server["password"]
    )
    sftp = ssh.open_sftp()
    ensure_remote_dir(sftp, ftp_server["directory"])

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    service_client = TableServiceClient.from_connection_string(conn_str=azure["connection_string"])

    for table in azure["tables"]:
        table_name = table["name"]
        fields = [field["name"] for field in sorted(table["fields"], key=lambda x: x["order"])]
        print(f"Processing table {table_name}...")

        table_client = service_client.get_table_client(table_name=table_name)
        entities = table_client.query_entities(query_filter="", select=fields)

        row_count = 0
        file_count = 1

        output_buffer = BytesIO()
        text_wrapper = TextIOWrapper(output_buffer, encoding='utf-8', newline='')
        writer = csv.DictWriter(text_wrapper, fieldnames=fields)
        writer.writeheader()

        for entity in entities:
            row_data = {field: extract_value(entity.get(field, "")) for field in fields}
            if not is_meaningful_row(row_data):
                continue

            writer.writerow(row_data)
            row_count += 1

            if row_count % 1000000 == 0:
                text_wrapper.flush()
                output_buffer.seek(0)

                filename = f"{table_name}_{timestamp}_{file_count}.csv"
                file_count += 1

                sync_event = {
                    "scheduleId": schedule_id,
                    "tableName": table_name,
                    "tableId": table["id"],
                    "fileName": filename,
                    "rowCount": row_count,
                    "lastRowId": None,
                    "lastRowTimestamp": None,
                    "startedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "finishedAt": None,
                    "success": False,
                    "log": "Sync started"
                }

                try:
                    remote_path = f"{ftp_server['directory']}/{filename}"
                    sftp.putfo(output_buffer, remote_path)
                    print(f"Uploaded chunk: {filename}")
                except Exception as e:
                    print(f"[ERROR] Failed to upload chunk {filename}: {e}")

                output_buffer = BytesIO()
                text_wrapper = TextIOWrapper(output_buffer, encoding='utf-8', newline='')
                writer = csv.DictWriter(text_wrapper, fieldnames=fields)
                writer.writeheader()

            if row_count % 1000 == 0:
                print(f"\rRows processed: {row_count}", end="", flush=True)

        if output_buffer.tell() > 0 and row_count % 1000000 != 0:
            text_wrapper.flush()
            output_buffer.seek(0)

            filename = f"{table_name}_{timestamp}_{file_count}.csv"
            sync_event = {
                "scheduleId": schedule_id,
                "tableName": table_name,
                "tableId": table["id"],
                "fileName": filename,
                "rowCount": row_count,
                "lastRowId": None,
                "lastRowTimestamp": None,
                "startedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "finishedAt": None,
                "success": False,
                "log": "Sync started"
            }

            try:
                remote_path = f"{ftp_server['directory']}/{filename}"
                sftp.putfo(output_buffer, remote_path)
                print(f"Uploaded final: {filename}")
            except Exception as e:
                print(f"[ERROR] Failed to upload final file {filename}: {e}")

    sftp.close()
    ssh.close()
    conn.close()
    print("All tables processed and uploaded.")


if __name__ == "__main__":
    main()
