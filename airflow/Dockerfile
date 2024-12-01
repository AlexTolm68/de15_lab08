
FROM python:3.8.13

COPY ./requirements.txt /usr/local/airflow/requirements.txt

WORKDIR /usr/local/airflow

RUN pip install --upgrade pip
RUN pip install --user -r requirements.txt
RUN pip install adbc_driver_manager
RUN pip install adbc-driver-postgresql pyarrow

RUN mkdir -p /usr/local/airflow/dags

ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

COPY airflow.cfg /usr/local/airflow/airflow.cfg
COPY ./dags/* /usr/local/airflow/dags/

# меняем тип эксзекутора для Airflow на LocalExecutor (запускает задачи параллельно, но только на одной машине)
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
# указываем подключение к постгре
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
# отключаем загрузку примеров дагов
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# отключаем загрузку соединений по умолчанию
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
# и еще два флажка, полезных при отладке
# отображать имя хоста в логах
ENV AIRFLOW__CORE__EXPOSE_HOSTNAME=True
# отображать трассировку стека при возникновении ошибки
ENV AIRFLOW__CORE__EXPOSE_STACKTRACE=True

