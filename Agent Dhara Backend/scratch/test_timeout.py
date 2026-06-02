import os
import sys
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Documents\DHARA-GX\Agent Dhara Backend"
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from agent.azure_sql_executor import get_connection

print("Connecting...")
conn = get_connection()
print("Setting connection.timeout = 15")
conn.timeout = 15
print("Connection timeout is:", conn.timeout)

cursor = conn.cursor()
print("Has connection attribute:", hasattr(cursor, "connection"))
print("Is cursor.connection same as conn:", cursor.connection is conn)

# Try setting timeout on cursor
try:
    cursor.timeout = 15
    print("Successfully set cursor.timeout!")
except Exception as e:
    print("Failed to set cursor.timeout:", type(e).__name__, str(e))

cursor.close()
conn.close()
