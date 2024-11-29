#import os - понадобится для подтягивания кредов в идеале, если успеем завезти фичу с прокидыванием их в окружение
import io
import zipfile
import boto3
from botocore.exceptions import ClientError
import polars as pl
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {"owner": "lab08_team", "depends_on_past":True}

@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)

def lab08_browser_events():

    @task
    def grab_s3_data():
        # maybe put lab imports here so scheduler to feel better
        data_object = 'browser_events.jsonl'
        postgresql_conn = 'postgresql://nplstudent:newprolab@postgres-db:5432/de15_lab08'
        context = get_current_context()
        start = context["data_interval_start"]
        # maybe put lab imports here so scheduler to feel better

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id='Y***E',
            aws_secret_access_key='Y***M'
            # aws_access_key_id = os.environ['AWS_SECRET_ID'],- понадобится для подтягивания кредов в идеале, если успеем завезти фичу с прокидыванием их в окружение
            # aws_secret_access_key = os.environ['AWS_SECRET_KEY']- понадобится для подтягивания кредов в идеале, если успеем завезти фичу с прокидыванием их в окружение
        )

        # Получить список объектов в бакете
        # for key in s3.list_objects(Bucket='npl-de15-lab8-data')['Contents']:
        #    print(key['Key'])
        # Получить объект
        print(
            f'getting from key: year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/{data_object}.zip')
        try:
            get_object_response = s3.get_object(Bucket='npl-de15-lab8-data',
                                                Key=f'year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/{data_object}.zip')
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
                            print('polars time!')
                            pl_json_df = pl.read_ndjson(io.StringIO(decoded_json))
                            print(pl_json_df)
                            print(f'writing df to postgres: public.{subfile[:-6]}')
                            try:
                                pl_json_df.write_database(
                                    table_name=f'public.{subfile[:-6]}',
                                    connection=postgresql_conn,
                                    engine='adbc',
                                    if_table_exists='append'
                                )
                            except:
                                pl_json_df.write_database(
                                    table_name=f'public.{subfile[:-6]}',
                                    connection=postgresql_conn,
                                    engine='adbc'
                                )

        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('No object found - returning empty')
            else:
                raise


    grab_s3_data()


actual_dag = lab08_browser_events()

