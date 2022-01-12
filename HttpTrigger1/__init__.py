import logging
import azure.functions as func

from datetime import datetime, timedelta, timezone

from pypika import Query, Table, Order
from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path
from pandas import to_datetime
from typing import List

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    
    logging.info('Python HTTP trigger function processed a request.')

    curr_filepath = Path(__file__).resolve().parent
    SCOPES = ['https://www.googleapis.com/auth/bigquery']
    SERVICE_ACCOUNT_FILE = f'{curr_filepath}/mixidea-91a20-000af8d05383.json'
    log = Table("mixidea-91a20.mixidea_data2.shared_log3")

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    
    sql_expr = Query.from_(log).select("*").where(
                log.user_name == 'Vincent'
                ).get_sql(quote_char=None)
    
    logging.info(sql_expr)
    logging.info('ssss dd')

    raw_data = list(bigquery_client.query(sql_expr).result())
    
    logging.info('ttt dd')
    data: List[Log] = [Log(**item).dict() for item in raw_data]


    name = req.params.get('name')
    if not name:
        try:
            print('ccc')
            req_body = req.get_json()
            print(req_body)
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        print('bbb')
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        print('aaa')
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
