import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.azure_sql_executor import get_connection

def main():
    try:
        conn = get_connection()
        conn.autocommit = False
        cursor = conn.cursor()
        print("Connected successfully!")
        
        # 1. Clean up old test table if it exists
        cursor.execute("IF OBJECT_ID('dbo.TestTable', 'U') IS NOT NULL DROP TABLE dbo.TestTable")
        
        # 2. Create the test table
        print("Creating table dbo.TestTable...")
        cursor.execute("CREATE TABLE dbo.TestTable (id INT PRIMARY KEY, name VARCHAR(50))")
        
        # 3. Insert a row
        print("Inserting row...")
        cursor.execute("INSERT INTO dbo.TestTable (id, name) VALUES (1, 'Test Row')")
        
        # 4. Query before commit
        cursor.execute("SELECT * FROM dbo.TestTable")
        row = cursor.fetchone()
        print(f"Row queried before commit: {row}")
        
        # 5. Commit
        print("Committing transaction...")
        conn.commit()
        print("Committed!")
        
        # 6. Query after commit
        cursor.execute("SELECT * FROM dbo.TestTable")
        row = cursor.fetchone()
        print(f"Row queried after commit: {row}")
        
        # 7. Check sys.tables
        cursor.execute("SELECT name FROM sys.tables WHERE name = 'TestTable'")
        table_found = cursor.fetchone()
        print(f"Table found in sys.tables: {table_found}")
        
        # 8. Clean up
        cursor.execute("DROP TABLE dbo.TestTable")
        conn.commit()
        print("Test complete and cleaned up.")
        
        conn.close()
    except Exception as e:
        print("Error during write test:", e)

if __name__ == "__main__":
    main()
