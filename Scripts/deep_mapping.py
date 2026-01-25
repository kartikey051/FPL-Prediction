from Utils.db import execute_query
import json

def get_schema(table_name):
    try:
        res = execute_query(f"DESCRIBE {table_name}")
        return {r['Field']: r['Type'] for r in res}
    except Exception as e:
        return str(e)

def get_sample(table_name, cols, n=5):
    try:
        col_str = ", ".join(cols[:5]) # limit columns for sample
        return execute_query(f"SELECT {col_str} FROM {table_name} LIMIT {n}")
    except:
        return []

def main():
    tables_res = execute_query("SHOW TABLES")
    tables = [list(t.values())[0] for t in tables_res]
    
    mapping = {}
    for table in tables:
        schema = get_schema(table)
        mapping[table] = {
            "columns": schema,
            "sample": get_sample(table, list(schema.keys())) if isinstance(schema, dict) else []
        }
        
    with open("deep_schema_mapping.json", "w") as f:
        json.dump(mapping, f, indent=4)
    print("Mapping saved to deep_schema_mapping.json")

if __name__ == "__main__":
    main()
