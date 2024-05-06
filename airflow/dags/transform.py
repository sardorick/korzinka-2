import json
from database_utils import connect_to_database

def transform_data(product_id):
    """
    Fetches raw JSON data for a product from the 'products' table, 
    parses it, and extracts nutritional data to populate the 'nutritional_facts' table.

    Args:
        product_id (int): The ID of the product in the 'products' table.
    """

    conn = connect_to_database()
    cursor = conn.cursor()

    # Fetch raw_json from products table
    sql = "SELECT raw_json FROM products WHERE product_id = %s"
    cursor.execute(sql, (product_id,))
    result = cursor.fetchone()

    if result:
        raw_json_data = result[0]

        try:
            # Since raw_json is already a dictionary, load directly
            product_data = raw_json_data  

            # Extract nutrients 
            nutrients = product_data.get('product', {}).get('nutriments', {})

            for nutrient_name, nutrient_data in nutrients.items():
                # Handle potential missing values
                if isinstance(nutrient_data, dict): 
                    nutrient_value = nutrient_data.get('value')
                    nutrient_unit = nutrient_data.get('unit')  
                else: 
                    nutrient_value = nutrient_data 
                    nutrient_unit = None  # Set a default if no unit is found
                if nutrient_value is None:
                        nutrient_value = 0
                        print(f"Warning: Missing value for '{nutrient_name}' in product ID: {product_id}. Imputed with 0.")

                if isinstance(nutrient_value, float):  
                    if nutrient_value < 0:  # Check for negative invalid
                        print(f"Warning: Invalid value for '{nutrient_name}': {nutrient_value} in product ID: {product_id}.")
                # print(nutrient_name, nutrient_data)

                # Insert into nutritional_facts table
                sql = """
                    INSERT INTO nutritional_facts (product_id, nutrient_name, nutrient_value, nutrient_unit)
                    VALUES (%s, %s, %s, %s)
                """
                data = (product_id, nutrient_name, nutrient_value, nutrient_unit)
                # print("Data Tuple:", data) 

                cursor.execute(sql, data)

            conn.commit()
            print(f"Successfully transformed data for product ID: {product_id}")
        except KeyError as e:  # Catch more specific KeyErrors
            print(f"Error processing product ID: {product_id} ({e})")
    else:
        print(f"Product ID not found: {product_id}")

    cursor.close()
    conn.close()

# if __name__ == "__main__":
#     transform_data(2) 
