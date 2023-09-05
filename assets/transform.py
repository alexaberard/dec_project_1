from sqlalchemy.engine import Engine
from jinja2 import Template

def transform(engine: Engine, sql_template: Template, table_name: str):
    sql_query = f"""
            drop table if exists {table_name};
            create table {table_name} as ({sql_template.render()});
        """
    engine.execute(sql_query)