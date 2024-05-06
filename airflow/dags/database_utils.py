import psycopg2
from dotenv import load_dotenv
import os 
import json

def connect_to_database():
    """Establishes a connection to the PostgreSQL database."""

    load_dotenv()  
    conn = psycopg2.connect(
        host=os.getenv('POSTGRESQLHOST'),
        database=os.getenv('POSTGRESQLDATABASE'),
        user=os.getenv('POSTGRESQLUSER'),
        password=os.getenv('POSTGRESQLPASSWORD')
    )
    return conn

def save_to_database(product_data):
    """Saves product data into the 'products' table."""

    conn = connect_to_database()
    cursor = conn.cursor()

    sql = """
        INSERT INTO products (barcode, product_name, nutrition_grades, raw_json)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (barcode) DO UPDATE 
            SET product_name = excluded.product_name,
                nutrition_grades = excluded.nutrition_grades,
                raw_json = excluded.raw_json; 
    """
    data = (
        product_data["code"],
        product_data.get("product", {}).get("product_name"),
        product_data.get("product", {}).get("nutrition_grades"),
        json.dumps(product_data) 
    )

    cursor.execute(sql, data)
    conn.commit()
    conn.close()
