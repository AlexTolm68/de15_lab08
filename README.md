# de15_lab08

## Запуск  docker-compose stek posgresql-airflow-superset

```
docker compose --env-file=env.cfg -f docker-compose-non-dev.yml -f docker-compose-airflow-posgres.yml up -d
``` 
- docker-compose-non-dev.yml - superset
- docker-compose-airflow-posgres.yml - airflow, posgres_db

Параметры конфигурации, логины и пароли необходимо передовать в файле env.cfg

## Остановка docker-compose

```
docker compose -f docker-compose-non-dev.yml -f docker-compose-airflow-posgres.yml stop
``` 
Добавить admin пользователя AirFlow, инструкция находится ниже.


Все переменные для окружения AirFlow и Posgres находятся в файле env.cfg в корневой директории.

Dockerfile для airflow, posgresql, а также ДАГи  расположены в директории /airflow

## Добавить файлы Superset и файл docker-compose-non-dev.yml необходимо:

В любой директории выполнить
```
git clone https://github.com/apache/superset
```
Затем скопировать все файлы из папки superset в корневую директорию проекта de15_lab08

рекомендуем вам переключиться на стабильный релиз
найти их можно тут: https://github.com/apache/superset/tags
например, на 4.0.1:

```
git checkout 4.0.1
```

Создать файл и добавить зависимости
```
touch ./docker/requirements-local.txt
echo "psycopg2-binary" >> ./docker/requirements-local.txt
```

___________________________________________________________________________________________

Пока все в упрощенном виде в попытке скорее всем обеспечить доступ к полному набору данных.

Что сейчас есть:
- Поднимается докер компоуз с аирфлоу на порту 8082 и постгрес-ДБ на порту 5438
- Даг смотрит на креды, которые жестко в него прописаны (пока заменил звездочками)
- Можем получить список ключей (чтобы можно было при циклом сразу все данные пролить до реализации правильной логики дага с почасовым запуском)
- Можем получить .zip по конкретному ключу
- Умеем разархировать этот .zip в бинарную строку - там джысончик
- Декодим строку в utf-8

Сейчас нужно:
- Добить часть, где джысон скармливается поларису
- Загружать полученный датафрейм в постгрес на порт 5438
- Довезти логику из дагов по 5 лабе, чтобы определять ключ на основе каждого часа. И делать несколько параллельных тасок, которые в этот час тянут из с3 ключ с датой и временем для geo_events, geo_events и тд

Чтобы всё заработало надо классический набор действий:

```
docker compose up
```

```
docker compose exec airflow-webserver bash
```

```
airflow users create -u admin -f Ad -l Min -r Admin -e admin@adm.in
```

Потом в cd dags/lab08_get_s3.py подложить свои креды, которые можно сделать <a href="https://yandex.cloud/ru/docs/iam/operations/sa/create-access-key#console_1">по инструкции</a> (ну или для тестов наверное можете и мои попросить, если долго-муторно)
Дальше просто триггерим даг в веб-интерфейсе уже

Попозже будет нужно: 
- Добавить в этот же даг последовательную таску, которая уже ходит в постгрес, всё забирает и каким-нибудь поларсом (или как угодно) крутит данные, чистит, сохраняет в постгрес в FACT-слой
- Потенциально ещё отдельный даг, который потом агрегаты считает на пролитых данных (или последовательный, если там везде агрегаты совпадать будут по часу)


Совсем потом нужно будет не забыть:
- Заменить логику получения кредов например на передачу из при старте докера через -e и отбор уже в dag через os.environ
- Проинспектировать и вычистить файлы докера
- Проверить скрипт дага, убрать лишее

Потом ещё пригодится:
```
airflow dags backfill lab08 -s 2020-11-15 -e 2020-11-??
```
