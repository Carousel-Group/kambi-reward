from requests.exceptions import HTTPError
import io
import json
import pandas as pd
import requests
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import pytz
from datetime import datetime 
from requests import get
import numpy as np


def upload_data_bq(table_id, df,append_truncate):

    data_types = {
        'object' : 'STRING',
        'int64' : 'INTEGER',
        'float64' : 'FLOAT',
        'bool' : 'BOOL',
        'datetime64' : 'DATETIME'
    }

     
    credentials = service_account.Credentials.from_service_account_file(
        "keyfile.json", scopes=["https://www.googleapis.com/auth/cloud-platform"]
                  )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
  
    table_id = table_id
    columns = df.dtypes.replace(data_types)
    column_names = columns.index
    column_types = columns.values
    print(column_types)

    schema = []
    for column_name, column_type in zip(column_names, column_types):
        
        if column_type == 'STRING':
            schema.append(bigquery.SchemaField(column_name, column_type))
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition = append_truncate
    )

    job = client.load_table_from_dataframe(df,table_id, job_config=job_config)
    print('Uploading data to Bigquery...')
    job.result() #Wait for result of the job
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {} in Stage.".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    print(table.num_rows)
    print(len(table.schema))
    print(table_id)
    
def merge_incremental_results(resource):
    
    credentials = service_account.Credentials.from_service_account_file(
    "keyfile.json", scopes=["https://www.googleapis.com/auth/cloud-platform"]
                  )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
  
 

    query = 'CALL `cg-maximbet-bi.data.bmc_{}`()'.format(resource)

    try:
        job = client.query(query=query, location='us-central1')
        print('Merging results in DataModel...calling routines')
        job.result() # Wating the completition of the Job
        print("This procedure processed {} bytes.".format(job.total_bytes_processed))
        print(job.total_bytes_processed)
        result = job.result()
        return result.total_rows, job.total_bytes_processed

    except Exception as err:
        print(f'An error occurred: {err}')
        sys.exit(1)


##url = "https://operator-api.ext.kambi.com/message-feed/api/reward/maximbet/v1/from/1"
##url_1 = f'https://operator-api.ext.kambi.com/message-feed/api/reward/maximbet/v1/from/{}'
url = 'https://operator-api.ext.kambi.com/message-feed/api/reward/maximbet/v1/latest'


certs = 'cert_kambi_prod.pem','private_key_kambi_prod.pem' 
headers = {'Content-type': 'application/json'}
destination = "cg-maximbet-bi.stage.kambi_bmc_rewards"
destination_logs = "cg-maximbet-bi.data.kambi_bmc_logs"
host = 'https://operator-api.ext.kambi.com/message-feed/api'       
resource = 'reward' 


try:
    ip = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ip))
    print(f"current url is: {host}")
    print(f"current resource is {resource}")
    print(f'Version: 1 - Build: 2')
    print(certs)

 
except Exception as err:
        print(f'Other error occurred: {err}')
        
def main():
    try:
        start_time = time.time()
        url_messageID = '{0}/{1}/maximbet/v1/latest'.format(host,resource)
        r_messageId = requests.get(url_messageID, headers=headers, cert=certs)
        r_messageId.raise_for_status()
        output_messageId = r_messageId.json()
        df_messageId = pd.json_normalize(output_messageId,record_path=['messages'])
        message_id = df_messageId.messageId[0] - 5000
        
        url = '{0}/{1}/maximbet/v1/from/{2}?batchSize=5000'.format(host,resource,message_id)
        r = requests.get(url, headers=headers, cert=certs)
        r.raise_for_status()
        output = r.json()

        df = pd.json_normalize(output,record_path=['messages'])

        print("messages OK")
        df.columns = ['messageId', 'messageType', 'timestamp', 'rewardId',
           'rewardType', 'customerPlayerId', 'status',
           'activationDate', 'expirationDate', 'couponRef',
           'rewardTemplateId', 'isGroupReward', 'currency',
           'boostPercentage',
           'maxStake',
           'maxExtraWinnings',
           'eventIds', 'betBuilder',
           'minCombinationOdds', 'regulations',
           'tags','eventGroupIds']
        
        df.messageId = df.messageId.astype(np.int64)              
        df.messageType  = df.messageType.map(str)           
        df.timestamp   = df.timestamp.map(str)            
        df.rewardId  = df.rewardId.astype(np.int64)
        df.rewardType   = df.rewardType.map(str)          
        df.customerPlayerId  = df.customerPlayerId.map(str)     
        df.status  = df.status.map(str)               
        df.activationDate   = df.activationDate.map(str)       
        df.expirationDate  = df.expirationDate.map(str)        
        df.couponRef   = df.couponRef.astype(np.int64)            
        df.rewardTemplateId = df.rewardTemplateId.astype(np.int64)      
        df.isGroupReward  = df.isGroupReward.map(str)          
        df.currency  = df.currency.map(str)               
        df.boostPercentage  = df.boostPercentage.astype(np.int64)       
        df.maxStake  = df.maxStake.astype(np.float64)           
        df.maxExtraWinnings = df.maxExtraWinnings.astype(np.float64)
        df.eventIds = df.eventIds.map(str)             
        df.betBuilder  = df.betBuilder.map(str)             
        df.minCombinationOdds = df.minCombinationOdds.astype(np.float64)
        df.regulations = df.regulations.map(str)           
        df.tags = df.tags.map(str)
        df.eventGroupIds = df.eventGroupIds.map(str)
    
        print(df.dtypes)
    
        upload_data_bq(destination,df,"WRITE_TRUNCATE")
        print("df uploaded to stage")
        
        merge_results = merge_incremental_results(resource= resource)
        number_of_rows = merge_results[0]
        bytes_processed = merge_results[1]
        processingTime = (time.time() - start_time)
        print(number_of_rows,bytes_processed,processingTime)
        
        df_logs = {
            "time":datetime.now(pytz.timezone('US/Eastern')).strftime("%m/%d/%Y, %H:%M:%S"), 
            "destination":resource, 
            "rows_added":number_of_rows,
            "bytes_processed":bytes_processed, 
            "processingTime":processingTime
            }
        
        df_logs = pd.DataFrame(df_logs, index = [0] )
        upload_data_bq(destination_logs,df_logs,"WRITE_APPEND")
    
    except HTTPError as http_err:
        print(f'HTTPError: {http_err}' )
        sys.exit(1)

        
    except Exception as err:
        print(f'Other error occurred: {err}')
        sys.exit(1)

if __name__ == '__main__':
    main()



