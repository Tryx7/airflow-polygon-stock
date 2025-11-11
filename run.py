# create reset_db.py and run it
import psycopg2
import os

conn = psycopg2.connect(
    host="pg-c63647-lagatkjosiah-692c.c.aivencloud.com",
    port="24862",
    database="defaultdb",
    user="avnadmin",
    password="AVNS_G1ajzCj_WUpXrLzc-3t",
    sslmode="require"
)

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS stock_market_data;")
conn.commit()
cur.close()
conn.close()
print("Table dropped successfully!")
