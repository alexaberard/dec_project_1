from sqlalchemy.engine import Engine
from jinja2 import Template, Environment
import os
from sqlalchemy import MetaData, Column, Table
from sqlalchemy.dialects import postgresql

def transform(engine: Engine, sql_template: Template, table_name: str):
    sql_query = f"""
            drop table if exists {table_name};
            create table {table_name} as ({sql_template.render()});
        """
    engine.execute(sql_query)
    
def load_source_table_to_dwh(
            sql_load_file: str,
            source_environment: Environment,
            source_engine: Engine,
            dwh_engine: Engine,
        ):
    table_name = os.path.splitext(sql_load_file)[0]
    sql_load_template  = source_environment.get_template(sql_load_file)
    data = [dict(row) for row in source_engine.execute(f"{sql_load_template.render()}").all()]
    
    print(data[0])
    print(sql_load_template.render())
    
    source_metadata = MetaData(bind=source_engine)
    source_metadata.reflect()    
    
    target_metadata = _create_table(table_name=table_name, metadata=source_metadata, engine=dwh_engine)
    table = target_metadata.tables[table_name]
    insert_statement = postgresql.insert(table).values(data[0])
    dwh_engine.execute(insert_statement)
    
    
def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [Column(column.name, 
                        column.type, 
                        primary_key=column.primary_key
        ) for column in existing_table.columns]
    new_table = Table(
        table_name, 
        new_metadata,
        *columns    
    )
    new_metadata.create_all(bind = engine)
    return new_metadata    