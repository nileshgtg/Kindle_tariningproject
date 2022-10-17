from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

dag=DAG(dag_id='etl_pipeline', start_date=datetime.today(), catchup=False, schedule_interval='@once')

def bqclient():
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    #Set table_id to the ID of the table to create.
    table_id = "focus-empire-363510.customer2.E-Commerce"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("InvoiceNo", "STRING"),
            bigquery.SchemaField("StockCode", "STRING"),
            bigquery.SchemaField("Description", "STRING"),
            bigquery.SchemaField("Quantity", "INTEGER"),
            bigquery.SchemaField("InvoiceDate", "STRING"),
            bigquery.SchemaField("UnitPrice", "FLOAT"),
            bigquery.SchemaField("CustomerID", "INTEGER"),
            bigquery.SchemaField("Country", "STRING"),
            ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://kindle_project2/data.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

gcs_to_bq_client= PythonOperator(task_id='gcs_to_bq_client', python_callable=bqclient, dag=dag)


data_cleaning_and_transformation = BigQueryOperator(task_id='data_cleaning_and_transformation',                      
                            sql="/sql_script.sql",
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> gcs_to_bq_client >> data_cleaning_and_transformation >> End
