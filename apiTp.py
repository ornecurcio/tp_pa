from fastapi import FastAPI,HTTPException
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import datetime
import psycopg2
import os
import json

# instalar en el entorno virtual 
# pip install fastapi
# pip install requests
# pip install numpy
# pip install pandas
# pip install psycopg2
# pip install python-dotenv
# pip install "uvicorn[standard]" 

# en solo dos comandos
# pip install fastapi python-dotenv pandas numpy psycopg2
# pip install "uvicorn[standard]"

# to run app es:  uvicorn nombre_archivo:app --reload

load_dotenv()

def connect_db():
    return psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"), 
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
app = FastAPI()


@app.get("/")
def root():
     return """
    Welcome / Bienvenido / Bienvenu \n
    For view documentation navigate to 'localhost:8000/docs'
    """

@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str):
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail= "Invalid model - Only accept 'product' or 'ctr'")
    try:
        engine = connect_db()
    except psycopg2.OperationalError:
        return {"error": "Database connection error"}
    cursor = engine.cursor()
    cursor.execute(f"""SELECT * FROM {model} WHERE advertiser_id = '{adv}';""")
    rows = cursor.fetchall()
    if  len(rows) == 0:
        raise HTTPException(status_code=404, detail= f"Advertiser '{adv}' not foun as active")
    # Verificar que el modelo es válido
    try:
        cursor.execute(f"""SELECT * FROM {model} WHERE advertiser_id = '{adv}' AND date = TO_DATE(%s, 'YYYY-MM-DD')""", (datetime.datetime.now().strftime('%Y-%m-%d'),))
        rows = cursor.fetchall()
        # Convertir el rows a un Json y devolverlo
        json_rows = json.dumps(rows, default=str)
        return json_rows
    except psycopg2.errors.NoDataFound:
        return {"error": f"No data found for adv {adv} and model {model}"}
    
@app.get("/stats/{model}")
def adv(model: str):
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail= "Invalid model - Only accept 'product' or 'ctr'")
    with connect_db() as engine:
        with engine.cursor() as cursor:
            try: 
                cursor.execute(f"""SELECT COUNT (advertiser_id) FROM {model} WHERE date = TO_DATE(%s, 'YYYY-MM-DD')""", (datetime.datetime.now().strftime('%Y-%m-%d'),))
            except psycopg2.errors.NoDataFound:
                return {"error": f"No data found for model {model}"}
            engine.commit()

    return

@app.get("/history/{adv}")
def rand(min: int = 0, max: int = 10):
    return f'Random number: {np.random.randint(min+1, max+1)}'

# @app.get("/{int}")
# def rand(min: int = 0, max: int = 10):
#     return f'Random number: {np.random.randint(min+1, max+1)}'

# class Item(BaseModel):
#     name: str
#     price: float

# @app.post("/create")
# def create_item(item: Item):
#     return f"New item created with name {item.name} and price {item.price}."

# @app.post("/cambalache/{number}")
# def cambalache(item: Item, number: int = Path(ge=4), q: str = Query("query", title="Algun param",
# min_length=3)):
#     result = {"number": number, "query": q, **item.dict()}
#     return result

# items = {1: "Item nro 1", 3: "Item nro 3"}
# @app.get("/item/{item_id}", status_code=status.HTTP_201_CREATED)
# def get_item(item_id: int):
#     if item_id not in items:
#         raise HTTPException(status_code=404, detail="Item not found")
#     return items[item_id]


# class Texto(BaseModel):
#     text: str
# @app.post("/texto")
# def create_item(item: Texto):
#     tts = gTTS(item.text)
#     tts.save('test.mp3')
#     return f"New mp3 file create from {item.text}"

# #     >>> 
# # >>> tts = gTTS('hello')
# # >>> tts.save('hello.mp3')
    

# 0MJGNPL5TP85CMYP807Y product

# 1GYAS7HDPACX993P201R ctr