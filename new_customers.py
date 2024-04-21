from kafka import KafkaProducer
from sqlalchemy import create_engine
import psycopg2 as pg
from dotenv import load_dotenv
import os
import pandas as pd
import json
import time

#importing the environments
load_dotenv()
user=os.getenv('user')
password=os.getenv('password')
dbname=os.getenv('db_name')
host=os.getenv('host_name')
port=os.getenv('port')


print('Load customers listening')

#TOPIC
NEW_CUSTOMERS_TOPIC="new_customers"

#define the producer
producer=KafkaProducer(
    bootstrap_servers="localhost:29092"
)
#connection to postgres db
connection=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
with connection.connect() as conn:
    i=0
    ids=[]
    ids.clear()
    while i<19:
        data=pd.read_sql('SELECT * FROM public."KAFKA_DATA" WHERE id_ = (SELECT MAX(id_) from public."KAFKA_DATA") and id_ is not null',conn)
        if len(data)>0:
            data=data[['id_','First Name','Last Name','Sex','Email','Phone']]
            id=data['id_'].unique()[0]
            if  id not in ids:
                data_=data.to_dict(orient="records")

                producer.send(
                    NEW_CUSTOMERS_TOPIC,
                    json.dumps(data_).encode('utf-8')
                )
                i+=1
                ids.append(id)
                ids=list(set(ids))
                print(f'Done sending id_{id}')
                # time.sleep(7)
                
            # else:
            #     print('Waiting for new customers')
            #     time.sleep(2)
            
        else :
            print('Waiting for new customers')
            time.sleep(3)
