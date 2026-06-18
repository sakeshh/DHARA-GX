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
        
        # 1. Drop old table/procedure if they exist
        cursor.execute("IF OBJECT_ID('dbo.Accounts_Clean', 'U') IS NOT NULL DROP TABLE dbo.Accounts_Clean")
        cursor.execute("IF OBJECT_ID('dbo.etl_clean_Accounts', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_Accounts")
        conn.commit()
        print("Dropped old table and procedure.")
        
        # 2. Try creating table
        print("Creating table dbo.Accounts_Clean...")
        create_table_sql = """
        CREATE TABLE [dbo].[Accounts_Clean] (
            [AccountID] BIGINT NOT NULL,
            [CustomerID] BIGINT NULL,
            [Balance] NVARCHAR(MAX) NULL,
            [AccountType] NVARCHAR(MAX) NULL,
            etl_created_at DATETIME DEFAULT GETDATE(),
            etl_updated_at DATETIME DEFAULT GETDATE(),
            etl_batch_id INT NULL,
            CONSTRAINT [PK_Accounts_Clean] PRIMARY KEY ([AccountID])
        )
        """
        cursor.execute(create_table_sql)
        print("Table created successfully!")
        
        # 3. Verify table in sys.tables
        cursor.execute("SELECT name FROM sys.tables WHERE name = 'Accounts_Clean'")
        print("Table in sys.tables:", cursor.fetchone())
        
        # 4. Commit table
        conn.commit()
        print("Table committed.")
        
        conn.close()
    except Exception as e:
        print("Error during test:", e)

if __name__ == "__main__":
    main()
