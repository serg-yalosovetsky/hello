import json
from pathlib import Path
from prefect import flow, task
import random
import mysql.connector
import paramiko
from azure.data.tables import TableServiceClient

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@task
def read_db_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, 'r') as f:
        return json.load(f)


@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    db_config = read_db_config()
    conn = mysql.connector.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    return f"Processed {customer_id}"

@flow
def send_marketing_email(
    mailing_lists: list[str],
    subject: str,
    body: str,
    test_mode: bool = False,
    attachments: list[str] | None = None
):
    """
    Send a marketing email blast to the given lists.

    Args:
        mailing_lists: A list of lists to email.
        subject: The subject of the email.
        body: The body of the email.
        test_mode: Whether to send a test email.
        attachments: A list of attachments to include in the email.
    """
    # Validate input
    if not mailing_lists:
        raise ValueError("mailing_lists must be provided")
    if not subject:
        raise ValueError("subject must be provided")
    if not body:
        raise ValueError("body must be provided")
    
    # Process each mailing list
    for mailing_list in mailing_lists:
        # Get customer IDs
        customer_ids = process_customer(mailing_list)
    


if __name__ == "__main__":
    send_marketing_email(
        mailing_lists=["list1", "list2"],
        subject="Test Email",
        body="This is a test email",
        test_mode=True
    )