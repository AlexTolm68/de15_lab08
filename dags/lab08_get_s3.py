import json
import io
import os
import zipfile
import boto3
import polars as pl
import pendulum
import ast
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {"owner": "lab08_team"}
TREE_FILE_TEMPLATE = '/tmp/tree_data_{}.csv'


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)
def lab08():
    @task
    def download_tree():
        context = get_current_context()
        tree_file = TREE_FILE_TEMPLATE.format(context["dag_run"].run_id)
        print(context["logical_date"])
        with open(tree_file, "w") as fp:
            print("interval,value", file=fp)
            print("start", context["data_interval_start"], file=fp, sep=',')
            print("end", context["data_interval_end"], file=fp, sep=',')

        return tree_file

    @task
    def grab_s3_data(tree_file_path):
        #maybe put lab imports here
        print("tree_file for this run:", tree_file_path)

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id = 'Y***E',
            aws_secret_access_key = 'Y***M'
            #aws_access_key_id = os.environ['AWS_SECRET_ID'],
            #aws_secret_access_key = os.environ['AWS_SECRET_KEY']
        )

        # Получить список объектов в бакете
        for key in s3.list_objects(Bucket='npl-de15-lab8-data')['Contents']:
            print(key['Key'])

        # Получить объект
        get_object_response = s3.get_object(Bucket='npl-de15-lab8-data',
                                            Key='year=2024/month=11/day=15/hour=23/browser_events.jsonl.zip')
        with io.BytesIO(get_object_response['Body'].read()) as tf:
            # rewind the file
            tf.seek(0)
            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:
                for subfile in zipf.namelist():
                    print(subfile)
                    with zipf.open(subfile) as jsonl_file:
                        jsonl_binary = jsonl_file.read()
                        print(jsonl_binary)
                        decoded_json = jsonl_binary.decode('utf-8')
                        print(decoded_json)
                        pl.read_json(io.StringIO(decoded_json))

    grab_s3_data(download_tree())


actual_dag = lab08()
