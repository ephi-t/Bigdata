# Import required libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#  Loading the dataset
file_path = "ECommerce_consumer_behaviour.csv"  # Ensure correct file path
df = pd.read_csv(file_path)

#  BEFORE CLEANING: Display initial dataset details
print("\n BEFORE CLEANING:")
print(f"Total Rows: {df.shape[0]}")
print("\nMissing Values:\n", df.isnull().sum())
print(f"\nDuplicate Rows: {df.duplicated().sum()}")
print("\nData Types Before Cleaning:")
print(df.dtypes)

#  Handle Missing Values
df["days_since_prior_order"] = df["days_since_prior_order"].fillna(0).astype(int)

#  Remove Duplicate Rows
df = df.drop_duplicates()

#  Ensure Correct Data Types
df["order_dow"] = df["order_dow"].astype(int)
df["order_hour_of_day"] = df["order_hour_of_day"].astype(int)

#  Standardize Text Columns
df["department"] = df["department"].str.lower().str.strip()
df["product_name"] = df["product_name"].str.lower().str.strip()

#  AFTER CLEANING: Display final dataset details
print("\n AFTER CLEANING:")
print(f"Total Rows After Cleaning: {df.shape[0]}")
print("\nMissing Values After Cleaning:\n", df.isnull().sum())
print(f"\nDuplicate Rows After Cleaning: {df.duplicated().sum()}")
print("\nData Types After Cleaning:")
print(df.dtypes)

#  Save cleaned data before loading into PostgreSQL
cleaned_file_path = "cleaned_ecommerce_data.csv"
df.to_csv(cleaned_file_path, index=False)
print(f"\n Cleaned data saved as '{cleaned_file_path}'")

# 🗄 PostgreSQL Connection Details (Modify as per your setup)
db_user = "postgres"
db_password = "1234"  # Change this to your actual PostgreSQL password
db_host = "localhost"
db_port = "5432"
db_name = "ecommerce_db"

#  Connect to PostgreSQL using psycopg2
conn = psycopg2.connect(
    dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
)
cursor = conn.cursor()

#  SQL query to explicitly create the table
create_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order INT,
    product_id INT,
    add_to_cart_order INT,
    reordered INT,
    department_id INT,
    department VARCHAR(50),
    product_name VARCHAR(255)
);
"""

# Execute the query
cursor.execute(create_table_query)
conn.commit()
print("\n Table 'orders' created successfully (if not already exists).")

# Close the cursor and connection
cursor.close()
conn.close()

# 🔗 Load cleaned data into PostgreSQL using SQLAlchemy
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
df = pd.read_csv(cleaned_file_path)  # Ensure we load the saved cleaned file
df.to_sql("orders", engine, if_exists="append", index=False)

print("\n Data successfully inserted into PostgreSQL!")
