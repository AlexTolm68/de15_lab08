# Мануал для дебага
### ниже приведен список возможных проблем и способ их преодоления

- если при запуске docker-compose файла выпадает ошибка:
    ```
    container_name: "<name>" is already exists
    ```
    попробуйте переименовать уже существующий образ контейнера с таким именем \
    или удалите его (особенно, если он больше не нужен) \
    Получить список существующих контейнеров можно командой
    ```
    docker ps -a
    ```
  
- если при загрузке данных даги падают с ошибкой:
  ```
  ModuleNotFoundError: required package 'adbc_driver_manager' not found.
  ```
  Зайдите в ваши airflow-scheduler и airflow-webserver в интерактивном режиме:
  ```
  docker exec -it container_id bash
  ```
  и пропишите поочередно команды:
  ```
  pip install adbc_driver_manager
  pip install adbc-driver-postgresql pyarrow
  ```