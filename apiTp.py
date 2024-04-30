from fastapi import FastAPI,HTTPException
import pandas as pd
import numpy as np
import datetime
# from pydantic import BaseModel

# instalar en el entorno virtual 
# pip install fastapi
# pip install gTTs
# pip install requests
# pip install numpy
# pip install "uvicorn[standard]" 

# to run app es:  uvicorn nombre_archivo:app --reload

app = FastAPI()

@app.get("/")
def root():
     return """
    <h1>Welcome / Bienvenido / Bienvenu</h1>
    <p>For view documentation navigate to <a href="localhost/documets">/docs</a></p>
    """

@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str):
    # Leer el archivo de advertiser_ids
    df_adv = pd.read_csv('files/advertiser_ids.csv')
    # Verificar que adv está en advertiser_id
    if adv not in df_adv['advertiser_id'].values:
        raise HTTPException(status_code=404, detail= f"Advertiser {adv} not foun as active")
    # Verificar que el modelo es válido
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail= "Invalid model - Only accept 'product' or 'ctr'")
    try:
        # Leer el archivo CSV
        if model == 'product':
            file_name = f"files/active/top_product_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
        else:
            file_name = f"files/active/top_ctr_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
        df = pd.read_csv(file_name)
        df = df[df['advertiser_id'] == adv]
        # Convertir el DataFrame a un diccionario y devolverlo
        return df.to_dict(orient='records')
    except FileNotFoundError:
        return {"error": f"No data found for adv {adv} and model {model}"}
    
@app.get("/stats/")
def item(item_id: int):
    return {"item_id": item_id}

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
    