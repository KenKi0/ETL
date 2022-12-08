## Работа с проектом

### Флаги для запуска ETL:
1. ```--ex-batch-size=1000``` - Count extracted data from primary database(PostgreSQL)
2. ```--ld-batch-size=1000``` - Count loaded data for one iteration of ETL
3. ```--freq=10``` - How often should the process be performed in minutes

### Запуск приложения локально
1. Установить зависимости командой
    ```$ poetry install```
2. Создать файл конфигурации ```.env``` в корне проекта и заполнить его согласно ```example.env ```
3. Первичный запуск ETL:
    ```$ python3 src/main.py --init```
4. Дальнейший запуск ETL:
    ```$ python3 src/main.py```

### Запуск приложения в docker
1. Создать файл конфигурации ```.env``` в корне проекта и заполнить его согласно ```example.env ```
2. Запустить контейнер командой
    ```$ docker-compose up -d --build```
