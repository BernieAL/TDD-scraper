import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="designer_products",
    user="admin",
    password="admin!"
)

# Create a cursor
cur = conn.cursor()

# Execute a SQL query
cur.execute("SELECT * FROM products;")

# Fetch and print the results
rows = cur.fetchall()
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()
