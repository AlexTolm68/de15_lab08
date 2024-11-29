# de15_lab08

Пока все в упрощенном виде в попытке скорее всем обеспечить доступ к полному набору данных.

Изменения по сравнению с предыдущей версией:
- Убрал из докерфайла конфликтную строку pip install Zipfile - все и без нее работает
- Допилил логику, которая забирает данные на основе времени запуска дага
- Допилил логику, которая кормит polars разархивированными жысонами из S3
- Допилил проливку данных в постгрес (в MVP-варианте) на порт 5438

Что сейчас есть:
- Поднимается докер компоуз с аирфлоу на порту 8082 и постгрес-ДБ на порту 5438
- Даг смотрит на креды, которые жестко в него прописаны (пока заменил звездочками)
- Забираем каждый час данных отдельно. Для каждого часа даг идет в s3 и циклом забирает данные для каждого из указанных в лабе объектов данных.
- Эти данные записываются в таблицы с соответствующими названиями.
**Дисклеймер: чтобы избежать ситуаций, когда несколько тасок пытаются одновременно обратиться к одной и той же задаче из поларса (и все ломают при попытке присвоить одинаковые индексы) я поставил ограничение - новый час не может считаться, пока не отработала задача по предыдущему часу. Это медленно. Но это работает **
  

Сейчас еще предстоит доработать в части airflow:
- Разделить один даг на 4: по одному для каждой таблицы. Тогда все заработает в 4 раза быстрее. А еще мы сможем запускать обновление для какой-то одной конкретной таблицы. Это важно, тк индемподентность тут обеспечить сложно.
- Загружать полученный датафрейм в постгрес на порт 5438


Чтобы всё заработало надо выполнить набор действий:

В даг вместо звездочек в атрибуты **aws_access_key_id** и **aws_secret_access_key** подложить свои креды, которые можно сделать <a href="https://yandex.cloud/ru/docs/iam/operations/sa/create-access-key#console_1">по инструкции</a> (ну или для тестов наверное можете и мои попросить, если долго-муторно):

```
sudo nano dags/lab08_get_s3.py
```
И все это дело запустить:
```
docker compose up
```

```
docker compose exec airflow-webserver bash
```

```
airflow users create -u admin -f Ad -l Min -r Admin -e admin@adm.in
```

```
airflow dags backfill lab08 -s 2024-11-15 -e 2024-11-27
```


Попозже будет нужно: 
- Добавить в этот же даг последовательную таску, которая уже ходит в постгрес, всё забирает и каким-нибудь поларсом (или как угодно) крутит данные, чистит, сохраняет в постгрес в FACT-слой
- Потенциально ещё отдельный даг, который потом агрегаты считает на пролитых данных (или последовательный, если там везде агрегаты совпадать будут по часу)

Для задач выше нам понадобятся аналитические скрипты из Dbeaver, я думаю их можно будет просто запихнуть в polars.sql() по аналогии со spark.sql()

Совсем потом нужно будет не забыть:
- Заменить логику получения кредов например на передачу из при старте докера через -e и отбор уже в dag через os.environ
- Проинспектировать и вычистить файлы докера
- Проверить скрипт дага, убрать лишее

Команды, которые могут пригодиться, если у вас капризный докер:

```
sudo systemctl restart docker.socket docker.service
```

```
docker rm $( docker ps -aq)
```


