## Working with a project

### Flags to launch ETL:
1. ```--ex-batch-size=1000``` - Count extracted data from primary database(PostgreSQL)
2. ```--ld-batch-size=1000``` - Count loaded data for one iteration of ETL
3. ```--freq=10``` - How often should the process be performed in minutes

### Running the application locally
1. Install dependencies by command:
    ```$ poetry install```
2. Create config file ```.env``` in the root of the project and fill it according to ```example.env ```
3. Initial launch ETL:
    ```$ python3 src/main.py --init```
4. Further launch ETL:
    ```$ python3 src/main.py```

### Running the application in docker
1. Create config file ```.env``` in the root of the project and fill it according to ```example.env ```
2. Launch container by command:
    ```$ docker-compose up -d --build```
