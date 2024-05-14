FROM python:3.9

WORKDIR /TP

COPY requirements.txt .
# otro copy con el codigo de la api
RUN pip install -r requirements.txt

COPY apiTp.py .


EXPOSE 80
# el entry poin o comand es ubicorn
ENTRYPOINT ["uvicorn", "apiTp:app", "--reload", "--host", "0.0.0.0", "--port", "80"]
