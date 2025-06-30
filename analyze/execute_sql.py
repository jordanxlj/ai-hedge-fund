import duckdb
import pandas as pd
import argparse
import sys
import os

def execute_analysis(db_path, table_name, sql_file, output_csv, common_logic_file='analysize/common_logic.sql'):
    """
    Executes a SQL query from a file against a DuckDB database and saves the results to a CSV.

    Args:
        db_path (str): The path to the DuckDB database file.
        table_name (str): The name of the table to use in the query.
        sql_file (str): The path to the SQL file containing the analysis query.
        output_csv (str): The path to save the resulting CSV file.
        common_logic_file (str): Path to the common SQL logic file to be included.
    """
    try:
        # Read the main SQL query from the file
        main_sql_query = read_sql_file(sql_file)
        
        # If the main query contains the common logic placeholder, replace it
        if '{{common_logic}}' in main_sql_query:
            if os.path.exists(common_logic_file):
                common_sql = read_sql_file(common_logic_file)
                # Ensure the final query is valid by removing the initial 'WITH' from the main query part
                # and replacing the placeholder.
                main_sql_query = main_sql_query.replace('{{common_logic}}', common_sql)
            else:
                print(f"Warning: Common logic file '{common_logic_file}' not found. Proceeding without it.")
                main_sql_query = main_sql_query.replace('{{common_logic}},', '')


        # Replace the table_name placeholder
        final_query = main_sql_query.replace('{{table_name}}', table_name)

        # Connect to the DuckDB database
        con = duckdb.connect(database=db_path, read_only=False) # Changed to read_only=False to allow CTEs/Views if needed

        # Execute the query and fetch the results into a pandas DataFrame
        print("Executing SQL query...")
        results_df = con.execute(final_query).fetchdf()
        print("Query finished. Saving results to CSV...")

        # Save the DataFrame to a CSV file with UTF-8 encoding
        results_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        
        print(f"Analysis complete. Results saved to '{output_csv}'")
        
        if 'plate_cluster' in results_df.columns:
            print("\nCluster Distribution:")
            print(results_df['plate_cluster'].value_counts())

    except FileNotFoundError:
        print(f"Error: The file '{sql_file}' or '{db_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()

def read_sql_file(filepath):
    """Reads a SQL file, trying UTF-8 and then GBK encoding."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        print(f"Warning: Could not read {filepath} as UTF-8. Trying GBK encoding...")
        with open(filepath, 'r', encoding='gbk') as f:
            return f.read()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL analysis on DuckDB and save results.")
    parser.add_argument("--db_path", required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--table_name", required=True, help="Name of the table to query.")
    parser.add_argument("--sql_file", required=True, help="Path to the SQL file.")
    parser.add_argument("--output_csv", required=True, help="Path for the output CSV file.")
    
    args = parser.parse_args()
    
    execute_analysis(args.db_path, args.table_name, args.sql_file, args.output_csv) 