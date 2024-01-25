import pendulum
import logging
import urllib.parse
from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")
test_dataset = Dataset("file://localhost/tmp/test_dataset.txt")


def write_to_file_func(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    file_path = urllib.parse.unquote(parsed_uri.path)
    with open(file_path, 'w') as f:
        f.write("row1\n")
        f.write("row2\n")
        f.write("row3\n")


def read_from_file_func(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    file_path = urllib.parse.unquote(parsed_uri.path)
    with open(file_path, 'r') as f:
        for line in f:
            logger.info("string in file: {}".format(line))


def remove_line_func(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    file_path = urllib.parse.unquote(parsed_uri.path)
    # Read the content of the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Exclude the first line
    lines = lines[1:]

    # Write the updated content back to the file
    with open(file_path, 'w') as file:
        file.writelines(lines)


with DAG(
    dag_id="test_dataset_master_producer",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    tags=["produces", "dataset-scheduled"],
) as dag1:
    # [START task_outlet]
    PythonOperator(
        task_id="producing_task",
        outlets=[test_dataset],
        python_callable=write_to_file_func,
        op_kwargs={"uri": test_dataset.uri}
    )
    # BashOperator(
    #     outlets=[test_dataset],
    #     task_id="producing_task",
    #     bash_command="sleep 3"
    # )
    # [END task_outlet]

with DAG(
    dag_id="test_dataset_worker_consumer",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=[test_dataset],
    tags=["consumes", "dataset-scheduled"],
) as dag2:
    # [START task_outlet]

    reading_task = PythonOperator(
        task_id="reading_task",
        # outlets=[test_dataset],
        python_callable=read_from_file_func,
        op_kwargs={"uri": test_dataset.uri}
    )

    consuming_task = PythonOperator(
        task_id="consuming_task",
        outlets=[test_dataset],
        python_callable=remove_line_func,
        op_kwargs={"uri": test_dataset.uri}
    )

    reading_task >> consuming_task
    # BashOperator(
    #     outlets=[test_dataset],
    #     task_id="consuming_task",
    #     bash_command="sleep 3"
    # )
    # [END task_outlet]