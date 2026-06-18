import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.azure_sql_executor import get_connection

def main():
    try:
        # 1. Connect, create table, commit, close
        conn1 = get_connection()
        cursor1 = conn1.cursor()
        print("Connection 1 established.")
        
        cursor1.execute("IF OBJECT_ID('dbo.PersistTest', 'U') IS NOT NULL DROP TABLE dbo.PersistTest")
        cursor1.execute("CREATE TABLE dbo.PersistTest (id INT)")
        cursor1.execute("INSERT INTO dbo.PersistTest (id) VALUES (42)")
        
        conn1.commit()
        print("Connection 1 committed and closing.")
        conn1.close()
        
        # 2. Open new connection, check table
        conn2 = get_connection()
        cursor2 = conn2.cursor()
        print("\nConnection 2 established.")
        
        cursor2.execute("SELECT name FROM sys.tables WHERE name = 'PersistTest'")
        table_found = cursor2.fetchone()
        print(f"Table in sys.tables: {table_found}")
        
        cursor2.execute("SELECT * FROM dbo.PersistTest")
        row = cursor2.fetchone()
        print(f"Row read in Connection 2: {row}")
        
        # Clean up
        cursor2.execute("DROP TABLE dbo.PersistTest")
        conn2.commit()
        print("Connection 2 cleaned up and closing.")
        conn2.close()
        
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
