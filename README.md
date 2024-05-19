# Sistema de Recomendación de Productos para Publicidad en Internet

## Descripción
Este repositorio contiene el código fuente de un sistema de recomendación de productos implementado como un servicio API, desarrollado como parte del trabajo práctico final para el curso de Programación Avanzada 2024. Este sistema personaliza las publicidades que los usuarios ven en internet, utilizando datos generados diariamente.

## Integrantes
- Curcio Ornela
- Gonzalez Sergio
- De los Angeles Torres Macarena

## Tecnologías Utilizadas
- FastAPI para el desarrollo de la API.
- PostgreSQL alojado en AWS RDS para la base de datos.
- Docker para la contenerización de la aplicación.
- Airflow para la orquestación de tareas.
- AWS EC2, AWS RDS, S3 y AWS ECS para despliegue y operaciones.

## Estructura del Repositorio
- `apiTp.py`: Archivo principal de la API.
- `dags/dag_tp.py`: DAG de Airflow para la orquestación de tareas.
- `rds.py`: Scripts para interacción con la base de datos PostgreSQL para realizar pruebas de practicas.
- `Dockerfile`: Para construir la imagen Docker de la API.
- `requirements.txt`: Dependencias de Python necesarias para el proyecto.
- `GenerateTPData.ipynd`: Notebook para generar csv con datos falso para usar como advertisers. 
- `files/`: Archivos de ejemplos generados previamente. 
- `Informe del trabajo práctico final de Programación Avanzada 2024.docx`: Documento que describe el proyecto en detalle.

## Configuración y Despliegue
### Requisitos
Asegúrate de tener Docker y Airflow instalados en tu sistema para ejecutar el proyecto localmente.  
Se requiere una cuenta en AWS para el despliegue en producción.

### Instalación Local
1. Clona el repositorio:
```bash
git clone https://github.com/ornecurcio/tp_pa.git
```
2. Crea una base de datos postgres en RDS o en tu maquina local usando las querys escritas en rds.py. 
2. Instala Airflow y cambia la direccion del directorio donde se almacenan los DAGs. 
3. Crea un bucker en S3 o modifica el codigo en dag.py para almacenar los csv en tu maquina local. 
5. Corre airflow: 
```bash
airflow webserver --port 8080 -D
airflow scheduler -D
```
4. Instala las dependencias en requirements.txt 
```bash
pip install -r requirements.txt
```
5. Corre la api en modo reload si vas a modificar el codigo y disfruta de su uso
```bash
uvicorn apiTp:app --reload
```

