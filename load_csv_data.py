from sqlalchemy import create_engine,URL
import pandas as pd 
from dotenv import load_dotenv
import os 
import logging,time

#setup logging
log=logging.getLogger()
logging.basicConfig()
log.setLevel(logging.INFO)


#importing the environments
load_dotenv()
user=os.getenv('user')
password=os.getenv('password')
dbname=os.getenv('db_name')
host=os.getenv('host_name')
port=os.getenv('port')

#url
connection=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')


#define the function
class PEOPLE:
    def __init__(self,csv_file: str,table_name: str) -> None:
        self.csv_file=csv_file
        self.table_name=table_name

    def read_csv(self)-> pd.DataFrame:
        data=pd.read_csv(self.csv_file).head(20)
        data['id_']=data.index+1
        data.sort_values(by='id_',ascending=True,inplace=True)
        return data
    
    def write_table(self, data: pd.DataFrame):
        total=0
        for j in range(1,len(data)):
            data_to_load=data[data['id_']==j]
            print(data_to_load)
            data_to_load.to_sql(self.table_name,con=connection,if_exists="append",index=False)
            total+=1
            print(f'Loaded {total} customers details for far')
            log.info('Data loaded successfully')
            time.sleep(7)


        
if __name__=='__main__':
    paramz=PEOPLE(
        '/Users/mac/Downloads/people-2000000.csv',
        'KAFKA_DATA'
    )
    paramz.read_csv()
    paramz.write_table(data=paramz.read_csv())
