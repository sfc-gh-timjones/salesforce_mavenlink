#!/usr/bin/env python3

import snowflake.connector
import os

def main():
    try:
        # Read YAML from file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(script_dir, "semantic_views", "opportunity_delivery_analytics.yaml")
        
        with open(yaml_path, 'r') as f:
            yaml_content = f.read()
        
        print(f"Reading YAML from: {yaml_path}")
        print(f"YAML content length: {len(yaml_content)} characters")
        
        # Connect using the coco connection
        conn = snowflake.connector.connect(connection_name="coco")
        cursor = conn.cursor()
        
        # Set context
        cursor.execute("USE ROLE ACCOUNTADMIN")
        cursor.execute("USE DATABASE ANALYTICS2")
        cursor.execute("USE SCHEMA MART")
        
        # Drop existing view
        cursor.execute("DROP SEMANTIC VIEW IF EXISTS opportunity_delivery_analytics")
        
        # Escape single quotes in YAML
        yaml_escaped = yaml_content.replace("'", "''")
        
        # Create semantic view
        sql = f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS2.MART', '{yaml_escaped}')"
        print(f"Executing SQL with {len(sql)} character SQL statement...")
        
        result = cursor.execute(sql)
        print("SUCCESS: Semantic view created successfully!")
        
        # Fetch and print result
        for row in cursor.fetchall():
            print(f"Result: {row}")
            
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
