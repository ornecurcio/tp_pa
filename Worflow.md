
Programacion
        Airflow 
            Local 
                comandos: 
                    AIRFLOW_VERSION=2.8.4
                    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
                    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
                    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
                    Crear la base de datos:
                        $ airflow db migrate
                    Crear un usuario para acceder a la web app:
                        $ airflow users create -u admin -f Ad -l Min -r Admin -e admin@example.com
                    Levantar el webserver:
                        $ airflow webserver --port 8080
                    Levantar el scheduler (en otra consola):
                        $ airflow scheduler
                hacer pipeline de datos con recomendaciones y que vaya a csv
                pipeline: 
                    1 - filtral clientes activos // done
                    2 - top producto por adv // done
                    3 - top ctr ( click / impresion ) por adv
            AWS 
                pasar los datos del pipeline a DRS
        Api

PASO INTERMEDIO
    Conectar Database con api 
    Conectar database con aiflow
Deployment
    hacer el docker de la api 
    hacer el EC2 de airflow 