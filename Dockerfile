FROM python:3.9

WORKDIR /TP

COPY requirements.txt .

RUN pip install -r requirements.txt

# el entry poin o comand es ubicorn
ENTRYPOINT ["jupyter", "notebook", "--port=8231", "--ip=0.0.0.0", "--allow-root"]
