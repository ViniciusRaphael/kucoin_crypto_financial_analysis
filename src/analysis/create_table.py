import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser

# Read configuration from 'config.ini' file
config = ConfigParser()
config.read('config.ini')

def read_csv(file_path):
    """
    Read CSV file into a DataFrame.
    
    Parameters:
    - file_path (str): Path to the CSV file.

    Returns:
    - pd.DataFrame: DataFrame containing the CSV data.
    """
    df = pd.read_csv(file_path)
    return df

def connect_to_database(connection_params):
    """
    Connect to the PostgreSQL database.
    
    Parameters:
    - connection_params (dict): Dictionary containing database connection parameters.

    Returns:
    - sqlalchemy.engine.Engine: SQLAlchemy engine for database connection.
    """
    try:
        engine = create_engine(f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")
        print('Connected to the database')
        return engine
    except Exception as e:
        print(f'Error: {e}')
        return None

def load_data_into_database(df, engine, table_name, if_exists='replace'):
    """
    Load DataFrame data into the PostgreSQL database.
    
    Parameters:
    - df (pd.DataFrame): DataFrame containing data to be loaded.
    - engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.
    - table_name (str): Name of the table to which data will be loaded.
    - if_exists (str): Specifies behavior when the table already exists. Default is 'replace'.

    Returns:
    - None
    """
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f'DataFrame data loaded into {table_name} table')
    except Exception as e:
        # Drop the table with CASCADE if an error occurs during loading
        drop_cascade_sql = (f'DROP TABLE {table_name} CASCADE')
        engine.execute(drop_cascade_sql)

def main():
    # Configuration
    db_connection = {
        'host': config.get('Database', 'host'),
        'port': config.getint('Database', 'port'),
        'database': config.get('Database', 'database'),
        'user': config.get('Database', 'user'),
        'password': config.get('Database', 'password')
    }
    csv_file_path = 'data/historical_crypto_data.csv'
    table_name = 'crypto_historical_price'

    # Read CSV file into a DataFrame
    df = read_csv(csv_file_path)

    # Connect to the PostgreSQL database
    engine = connect_to_database(db_connection)
    if engine is None:
        return

    # Load DataFrame data into the database
    load_data_into_database(df, engine, table_name)

if __name__ == "__main__":
    main()
