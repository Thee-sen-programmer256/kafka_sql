from kafka import KafkaConsumer
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os,json
import pandas as pd

#importing the environments
load_dotenv()
user=os.getenv('user')
password=os.getenv('password')
dbname=os.getenv('db_name')
host=os.getenv('host_name')
port=os.getenv('port')

connection=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')


# topis 
#TOPIC
NEW_CUSTOMERS_TOPIC="new_customers"

#define consumers 
consumer=KafkaConsumer(
    NEW_CUSTOMERS_TOPIC,
    bootstrap_servers="localhost:29092"
)


print('Accepted Customers listening.....')
customers_accepted_so_far=set()
id=0
while True:
    for message in consumer:
        consumed_message=json.loads(message.value.decode())
        print(consumed_message)
        consumed_message=pd.json_normalize(consumed_message)
        id+=1
        customers_accepted_so_far.add(id)
        consumed_message.to_sql("ACCEPTED_CUSTOMERS",con=connection,if_exists="append",index=False)
        print(f"so far added {len(customers_accepted_so_far)} to the database")




