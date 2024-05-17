FROM  python:3.10-slim-bullseye

WORKDIR /TP

COPY requirements.txt .
# Asume que estás usando una imagen base de Debian/Ubuntu
RUN apt-get update && apt-get install -y gcc
# otro copy con el codigo de la api
RUN pip install -r requirements.txt

COPY apiTp.py .

# el entry poin o comand es ubicorn
# el comando es ubicorn apiTp:app 
CMD ["uvicorn", "apiTp:app", "--host", "0.0.0.0", "--port", "8000"]

