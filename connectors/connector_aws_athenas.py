import datetime
import time
import json

import boto3
import urllib.parse

from src.lib.logs.logger import Logger

from setup import configs


class ConnectorAthenas:
    def __init__(self, endpoint_url=configs.get('ATHENA_ENDPOINT', 'https://aws.amazon.com/athena')):
        params = {}
        try:
            params['region_name'] = 'eu-west-1'
            #session = boto3.Session(profile_name='EUBI.Users')
            self.client = boto3.client('athena', **params)
        except ValueError:
            raise

    def run_script(self, ddl_script_path, s3_output, database, wait_for_done=False):

        results=None

        try:
            with open(ddl_script_path, 'r') as fp:
                ddl_script = fp.read()

            query_start = self.client.start_query_execution(
                QueryString=ddl_script,
                QueryExecutionContext={
                    'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': s3_output,
                }
            )

            QueryExecutionId = query_start['QueryExecutionId']

            # In case we want to wait for query to finish
            if wait_for_done:
                TIME_OUT = configs.get('QUERY_TIME_OUT')
                start_time = datetime.datetime.now()

                status = self.client.get_query_execution(QueryExecutionId = QueryExecutionId)
                status_str = status['QueryExecution']['Status']['State']

                end_time = datetime.datetime.now()

                difference = (end_time - start_time).total_seconds()

                while status_str in ['QUEUED', 'RUNNING'] or difference > TIME_OUT:
                    status = self.client.get_query_execution(QueryExecutionId = QueryExecutionId)
                    status_str = status['QueryExecution']['Status']['State']

                    end_time = datetime.datetime.now()
                    difference = (end_time - start_time).total_seconds()

                results = self.client.get_query_results(QueryExecutionId = QueryExecutionId)

        except Exception as ex:
            raise ex

        return results


if __name__ == '__main__':
    #Example
    cl = ConnectorAthenas()
    res = cl.run_script(ddl_script_path='../../sql/ddl_create_table_jira_all_issues.sql',database='hackathon_poc'
                      , s3_output='s3://eda-jira-hackathon/JiraHackathon/Athena', wait_for_done=True)
    print(res)
