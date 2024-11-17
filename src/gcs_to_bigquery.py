import os
import yaml
from google.cloud import bigquery, storage

# Load configuration
with open("config/config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

GCS_BUCKET_NAME = config["gcs_bucket"]
GCS_FILE_NAME = config["gcs_file"]
BIGQUERY_DATASET = config["bigquery"]["dataset"]
BIGQUERY_TABLE = config["bigquery"]["table"]
BIGQUERY_SCHEMA = config["bigquery"]["schema"]

# Initialize BigQuery and Storage clients
bigquery_client = bigquery.Client(project="turing-gadget-428216-t7", credentials="priyanka@turing-gadget-428216-t7.iam.gserviceaccount.com")
storage_client = storage.Client(project="turing-gadget-428216-t7", credentials="priyanka@turing-gadget-428216-t7.iam.gserviceaccount.com")


def create_table_if_not_exists():
    """Create a BigQuery table if it does not exist."""
    dataset_ref = bigquery_client.dataset(BIGQUERY_DATASET)
    table_ref = dataset_ref.table(BIGQUERY_TABLE)

    try:
        bigquery_client.get_table(table_ref)
        print(f"Table {BIGQUERY_TABLE} already exists.")
    except:
        schema = [bigquery.SchemaField(col["name"], col["type"], col["mode"]) for col in BIGQUERY_SCHEMA]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print(f"Table {BIGQUERY_TABLE} created in dataset {BIGQUERY_DATASET}.")

def load_data_from_gcs_to_bigquery():
    """Load data from a GCS bucket to a BigQuery table."""
    uri = f"gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True
    )

    load_job = bigquery_client.load_table_from_uri(
        uri, f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}", job_config=job_config
    )
    load_job.result()  # Wait for the job to complete
    print(f"Loaded data from {uri} into {BIGQUERY_DATASET}.{BIGQUERY_TABLE}")


def main():
    create_table_if_not_exists()
    load_data_from_gcs_to_bigquery()


if __name__ == "__main__":
    main()
