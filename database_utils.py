import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import psycopg2

class DatabaseConnector:
    """This class handles database connections and operations such as initializing the database engine,
    listing tables, and uploading dataframes to the database."""

    def __init__(self):
        pass

    def read_db_creds(self):
        """
        Reads database credentials from a YAML file and returns them as a dictionary.
        """
        file_path = "/Users/Kronks/Documents/AiCore/multinational_retail_data_centralisation/db_creds.yaml"
        with open (file_path) as creds_file:
            db_creds_dict = yaml.safe_load(creds_file)
            return db_creds_dict

    def init_db_engine(self): 
        """
        Initialises the database engine using the database credentials.
        """
        db_creds = self.read_db_creds()
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = db_creds['RDS_HOST']
        user = db_creds['RDS_USER']
        password = db_creds['RDS_PASSWORD']
        port = db_creds['RDS_PORT']
        database = db_creds['RDS_DATABASE']
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        return engine
    
    def list_db_tables(self):
        """
        Lists all the tables in the database and returns them as a list.
        """
        db_engine = self.init_db_engine()
        inspector = inspect(db_engine)
        tables = inspector.get_table_names()

        table_list = []

        for table_name in tables:
            table_list.append(table_name)
        
        return table_list

    def upload_to_db(self, df, table_name, db_credentials):
        """
        Uploads dataframes to specific database tables in PostgreSQL using the provided credentials.
        """
        conn_string = f"postgresql+psycopg2://{db_credentials['user']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['database']}"
        engine = create_engine(conn_string)

        df.to_sql(table_name, con=engine, if_exists="replace", index=False)

        
