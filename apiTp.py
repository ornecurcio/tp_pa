import base64
from io import BytesIO
from typing import List
from fastapi import FastAPI,HTTPException, Path, Query
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv
from datetime import datetime, timedelta
import psycopg2
import os

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

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
    <head>
        <title>TP PAGVD</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f0f0f0;
            }
            .container {
                position: relative;
                width: 80%;
                margin: auto;
                background-color: #fff;
                padding: 20px;
                box-shadow: 0px 0px 10px 0px rgba(0,0,0,0.1);
            }
            ul {
                list-style-type: none;
            }
            li {
                margin-bottom: 10px;
            }
            img {
                position: absolute;
                bottom: 0;
                right: 0;
                width: 100px;
                height: 100px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>TP Programación Avanzada para grandes volumentes de datos</h1>
            <h3>Autores:</h3>
            <ul>
                <li>Curcio, Ornela </li>
                <li>Gonzalez, Sergio</li>
                <li>Torres, Macarena</li>
            </ul>
            <h3>Profesores:</h3>
            <ul>
                <li>Mosteiro, Agustín</li>
                <li>Dinota, Matías</li>
            </ul>
            <h3>Documentación:</h3>
            <p>Para ver la documentación de la API navegar a <a href="/docs">/docs</a></p>
            Codigo fuente en <a href="https://github.com/ornecurcio/tp_pa">GitHub</a>
        </div>
    </body>
    </html>
    """

@app.get("/recommendations/{advertiser_id}/{model}", response_model=list)
def get_recommendations(
    advertiser_id: str = Path(..., description="The ID of the advertiser", example="RALVMMRCZ047ZH6W3HMC"),
    model: str = Path(..., description="The model type, either 'product' or 'ctr'", example="ctr"),
    date: datetime = Query(None, description="Date for which data is to be fetched. Format: YYYY-MM-DD", example="2024-05-15")
):
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail="Invalid model specified. Only 'product' or 'ctr' are acceptable.")
    
    target_date = date or (datetime.now() - timedelta(days=1)).date()
    
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {model} WHERE advertiser_id = %s AND date = %s;"
                cursor.execute(query, (advertiser_id, target_date))
                results = cursor.fetchall()
                if not results:
                    raise HTTPException(status_code=404, detail=f"No data found for advertiser '{advertiser_id}' on '{target_date}'")
                
                json_results = []
                for result in results:
                    if model == 'ctr':
                        json_results.append({
                            "date": result[0].isoformat(),
                            "advertiser_id": result[1],
                            "publisher_id": result[2],
                            "impressions": result[3],
                            "clicks": result[4],
                            "CTR": result[5]
                        })
                    else:
                        json_results.append({
                            "date": result[0].isoformat(),
                            "advertiser_id": result[1],
                            "product_id": result[2],
                            "count": result[3]
                        })
                return json_results
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/stats/variability/{model}")
def get_advertiser_variability(model: str = Path(..., description="The model type, either 'product' or 'ctr'", example="product"),):
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail="Invalid model specified. Only 'product' or 'ctr' are acceptable.")
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                cursor.execute(f"""
                    SELECT advertiser_id, COUNT(DISTINCT date) AS days_count
                    FROM {model}
                    GROUP BY advertiser_id
                    ORDER BY days_count DESC
                """)
                advertisers_variability = cursor.fetchall()
                if not advertisers_variability:
                    raise HTTPException(status_code=404, detail=f"No data found for model '{model}'. No advertisers found.")
                return {"advertisers_variability": advertisers_variability}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/stats")
def get_top_advertisers_by_ctr():
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                cursor.execute("""
                    SELECT advertiser_id, ROUND(AVG(ctr)::numeric, 2) AS average
                    FROM ctr
                    GROUP BY advertiser_id
                    ORDER BY average DESC
                    LIMIT 10
                """)
                top_adv_ctr = cursor.fetchall()
                return {"top_adv_ctr": top_adv_ctr}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    
@app.get("/stats/count/{model}")
def get_advertiser_count(model: str = Path(..., description="The model type, either 'product' or 'ctr'", example="ctr"),):
    if model not in ['product', 'ctr']:
        raise HTTPException(status_code=404, detail= "Invalid model specified. Only 'product' or 'ctr' are acceptable.")
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(DISTINCT advertiser_id) FROM {model};")
                count_advertiser = cursor.fetchone()[0]
                if count_advertiser is None:
                    raise ValueError(f"Failed to fetch advertiser count from '{model}'.")
                return {"count_advertiser": count_advertiser}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/history/ctr/{advertiser_id}/")
def get_ctr_advertiser_history(advertiser_id: str = Path(..., description="The ID of the advertiser", example="RALVMMRCZ047ZH6W3HMC")):
    yesterday = datetime.now() - timedelta(days=1)
    seven_days_ago = yesterday - timedelta(days=7)
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                query = f"""
                    SELECT product_id, ROUND(AVG(ctr)::numeric, 2) AS average
                    FROM ctr
                    WHERE advertiser_id = %s AND date >= %s
                    GROUP BY product_id
                    ORDER BY average DESC
                    LIMIT 10    
                """
                cursor.execute(query, (advertiser_id, seven_days_ago))
                results = cursor.fetchall()

                if not results:
                    raise HTTPException(status_code=404, detail=f"No data found for advertiser '{advertiser_id}' in the past 7 days in 'ctr' data.")

                recommendations = [{"product_id": row[0], "average_ctr": row[1]} for row in results]
                return {"recommendations": recommendations}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/history/product/{advertiser_id}/")
def get_product_advertiser_history(advertiser_id: str = Path(..., description="The ID of the advertiser", example="RALVMMRCZ047ZH6W3HMC")):
    yesterday = datetime.now() - timedelta(days=1)
    seven_days_ago = yesterday - timedelta(days=7)
    try:
        with connect_db() as engine:
            with engine.cursor() as cursor:
                query = f"""
                    SELECT product_id, COUNT(*) AS count
                    FROM product
                    WHERE advertiser_id = %s AND date >= %s
                    GROUP BY product_id
                    ORDER BY count DESC
                    LIMIT 10
                """
                cursor.execute(query, (advertiser_id, seven_days_ago))
                results = cursor.fetchall()

                if not results:
                    raise HTTPException(status_code=404, detail=f"No data found for advertiser ID {advertiser_id} in the past 7 days in 'product' data.")

                recommendations = [{"product_id": row[0], "count": row[1]} for row in results]
                return {"recommendations": recommendations}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/unique_vs_repeated_recommendations/{advertiser_id}")
def unique_vs_repeated_recommendations_ctr(advertiser_id: str = Path(..., description="The ID of the advertiser", example="RALVMMRCZ047ZH6W3HMC"), 
                                           start_date: datetime = Query(None, description="Date for which data is to be fetched. Format: YYYY-MM-DD",example="2023-05-01"), 
                                           end_date: datetime = Query(None, description="Date for which data is to be fetched. Format: YYYY-MM-DD",example="2023-05-10")):
    if not start_date:
        start_date = datetime.now() - timedelta(days=8)
    if not end_date:
        end_date = datetime.now() - timedelta(days=1)
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT product_id, ctr, date(date) as date
                    FROM ctr
                    WHERE advertiser_id = %s AND date >= %s AND date <= %s
                    ORDER BY date
                """, (advertiser_id, start_date.date(), end_date.date()))
                results = cursor.fetchall()
                if not results:
                    return {"error": "No data found for the specified range and advertiser."}
                product_details = {}
                for product_id, ctr, date in results:
                    if product_id not in product_details:
                        product_details[product_id] = {'total_ctr': 0, 'count': 0, 'dates': set()}
                    product_details[product_id]['total_ctr'] += ctr
                    product_details[product_id]['count'] += 1
                    product_details[product_id]['dates'].add(date)

                unique_ctr_total, repeated_ctr_total, unique_count, repeated_count = 0, 0, 0, 0
                for details in product_details.values():
                    avg_ctr = details['total_ctr'] / details['count']
                    if len(details['dates']) == 1:
                        unique_ctr_total += avg_ctr
                        unique_count += 1
                    else:
                        repeated_ctr_total += avg_ctr
                        repeated_count += 1
                avg_ctr_unique = unique_ctr_total / unique_count if unique_count else 0
                avg_ctr_repeated = repeated_ctr_total / repeated_count if repeated_count else 0
                return {
                    "average_ctr_unique": round(avg_ctr_unique, 2),
                    "average_ctr_repeated": round(avg_ctr_repeated, 2)
                }
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
