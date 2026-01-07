import psycopg2

DB_CONFIG = {
    "dbname": "solar_db",
    "user": "postgres",
    "password": "Abhyuday@postgresql", 
    "host": "127.0.0.1",
    "port": "5433"
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # List all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    
    tables = cursor.fetchall()
    print("Tables in database:")
    for table in tables:
        print(f"- {table[0]}")
        
    conn.close()

except Exception as e:
    print(f"Error: {e}")
