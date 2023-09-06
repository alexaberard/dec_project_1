from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql

class PostgreSqlClient:
    """
    A client for querying postgresql database. 
    """
    def __init__(self, 
        server_name: str, 
        database_name: str, 
        username: str, 
        password: str, 
        port: int = 5432
    ):  
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername = "postgresql+pg8000", 
            username = username,
            password = password,
            host = server_name, 
            port = port,
            database = database_name, 
        )

        self.engine = create_engine(connection_url)

    def write_to_table(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
        metadata.create_all(self.engine) # creates table if it does not exist 
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        self.engine.execute(upsert_statement)

    def table_exists(self, table_name: str) -> bool: 
        """
        Checks if the table already exists in the database. 
        """
        return inspect(self.engine).has_table(table_name)
    
    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)
    
    def run_sql(self, sql: str)-> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries. 
        This method should only be used if you expect a resultset to be returned. 
        """
        return [dict(row) for row in self.engine.execute(sql).all()]
    
    def upsert_in_chunks(self, data: list[dict], table: Table, metadata: MetaData, chunksize: int = 1000) -> None: 
        """
        Upserts data into a database table in chunks (e.g. 1000 rows at a time) in case of query timeouts or row limitations. 
        This method creates the table also if it doesn't exist. 
        """
        max_length = len(data)
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize
            self.write_to_table(data = data[lower_bound:upper_bound], table = table, metadata = metadata)