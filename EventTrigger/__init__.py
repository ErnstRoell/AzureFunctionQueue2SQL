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
    
    conn = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:jumboserver.database.windows.net,1433;Database=jumbo-simacan-db;Uid=jumboadmin;Pwd=ebbbcc6c-998d-48dc-88b0-c5541fdf16c9;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    c = conn.cursor()

    name = 'simacan'

    data = json.loads(msg.get_body().decode('utf-8'))

    df = pd.json_normalize(data,meta=['id','name','ppu','batters','type'],record_prefix='topping_', record_path=['topping'])
    cols = df.columns.values
    cols = [c for c in cols if c!='batters']
    d2 = json.loads(df.to_json(orient='records'))
    df = pd.json_normalize(d2,meta=cols,record_prefix='batter_', record_path=['batters','batter'])

    schema = pd.io.sql.get_schema(df, name)
    logging.info(schema)
    # c.execute(schema)
    # Insert Dataframe into SQL Server:
    for index, row in df.iterrows():
        logging.info("INSERT INTO {} ({}) values({})".format(name,",".join(df.columns),"'"+"','".join(row.astype(str))+"'"))
        try:
            c.execute("INSERT INTO {} ({}) values({})".format(name,",".join(df.columns),"'"+"','".join(row.astype(str))+"'"))
        except IntegrityError as e:
            logging.info('IntegrityError') 
            vals = ",".join([col+" = " + "'" + str(val) + "'" for col,val in zip(df.columns,row.astype(str))])
            keys = " and ".join([col+' = '+str(val) for col,val in zip(['id','topping_id','batter_id'],[str(row.id),str(row.topping_id),str(row.batter_id)])])
            logging.info(f"UPDATE {name} SET {vals} WHERE {keys}")
            c.execute(f"UPDATE {name} SET {vals} WHERE {keys}")      

    conn.commit()
    c.close()