import json
import logging
import azure.functions as func
import pandas as pd
import pypyodbc
from pypyodbc import IntegrityError
import random
import os


def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    # Fetch passwords and connection strings from the environment variables
    # Add your Computer Vision subscription key and endpoint to your environment variables.
    if 'DB_CONNECTION_STRING' in os.environ:
        db_connection_string = os.environ['DB_CONNECTION_STRING']
    else:
        logging.info("\nSet the DB_CONNECTION_STRING environment variable.\n Please add it to your local.settings.json.\n**Restart your shell or IDE for changes to take effect.**")
        sys.exit()
    
    # Here we create the connection to the database and initialize the     
    conn = pypyodbc.connect(db_connection_string)
    c = conn.cursor()

    # Database name
    name = 'simacan'

    # Fetch the data from the message body 
    data = json.loads(msg.get_body().decode('utf-8'))

    # Do first round of normalisation 
    df = pd.json_normalize(data,meta=['id','name','ppu','batters','type'],record_prefix='topping_', record_path=['topping'])
    cols = df.columns.values
    cols = [c for c in cols if c!='batters']

    # Normalize second array (repeat steps if more arrays exist)
    d2 = json.loads(df.to_json(orient='records'))
    df = pd.json_normalize(d2,meta=cols,record_prefix='batter_', record_path=['batters','batter'])

    # # Create the SQL query to initialize the database schema from the inferred datatypes in the DataFrame
    # schema = pd.io.sql.get_schema(df, name)
    # logging.info(schema)
    # # Send the query to the database
    # c.execute(schema)

    # Insert Dataframe into SQL Server:
    for index, row in df.iterrows():
        logging.info("INSERT INTO {} ({}) values({})".format(name,",".join(df.columns),"'"+"','".join(row.astype(str))+"'"))
        try:
            c.execute("INSERT INTO {} ({}) values({})".format(name,",".join(df.columns),"'"+"','".join(row.astype(str))+"'"))
        except IntegrityError as e: # Integrity error indicates duplicates exist. 
            logging.info('IntegrityError, trying an update') 
            vals = ",".join([col+" = " + "'" + str(val) + "'" for col,val in zip(df.columns,row.astype(str))])
            keys = " and ".join([col+' = '+str(val) for col,val in zip(['id','topping_id','batter_id'],[str(row.id),str(row.topping_id),str(row.batter_id)])])
            logging.info(f"UPDATE {name} SET {vals} WHERE {keys}")
            c.execute(f"UPDATE {name} SET {vals} WHERE {keys}")      

    conn.commit()
    c.close()