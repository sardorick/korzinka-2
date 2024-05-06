import requests
import database_utils as database_utils
API_BASE_URL = "https://world.openfoodfacts.org/api/v0/product/"
REQUIRED_FIELDS = "product_name,nutrition_grades,nutriments"

def get_product_data():
    """
    Fetches an unprocessed barcode from the database (there needs to be a database with available barcodes to check.), retrieves product data from the API, and saves it.
    """

    conn = database_utils.connect_to_database()
    cursor = conn.cursor()

    sql = "SELECT barcode FROM product_barcodes WHERE processed = FALSE LIMIT 1"
    cursor.execute(sql)
    result = cursor.fetchone()

    if result:
        barcode = result[0]

        url = f"{API_BASE_URL}{barcode}.json?fields={REQUIRED_FIELDS}"
        response = requests.get(url)

        if response.status_code == 200:
            product_data = response.json()
            if product_data['status'] == 1:
                database_utils.save_to_database(product_data) 

                # Mark barcode as processed 
                sql = "UPDATE product_barcodes SET processed = TRUE WHERE barcode = %s"
                cursor.execute(sql, (barcode,))
                conn.commit()

            else:
                print(f"Product not found: {barcode}")
        else:
            print(f"Error fetching product: {barcode} (Status: {response.status_code})")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    get_product_data()
