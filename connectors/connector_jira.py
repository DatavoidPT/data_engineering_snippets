# coding=utf-8
from atlassian import Jira
from setup import configs


class ConnectorJIRA:

    def __init__(self, endpoint_url=configs.get('JIRA_ENDPOINT', 'http://localhost:8080')
                 , username='admin'
                 , password='admin'):
        try:
            self.client = Jira(url=endpoint_url
                               , username=username
                               , password=password)
        except ValueError:
            raise

    def execute_jql_todict(self, string_jql, expand=None):
        try:
            data = self.client.jql(string_jql, '*all', 0, None, expand)
            return data
        except Exception as ex:
            print(ex)  #TODO: Logging
            return None

    def get_all_agile_boards(self, board_name=None):
        try:
            data = self.client.get_all_agile_boards(board_name)
            return data
        except Exception as ex:
            print(ex)  #TODO: Logging
            return None

    def get_all_sprint(self, board_id=None):
        try:
            data = self.client.get_all_sprint(board_id)
            return data
        except Exception as ex:
            print(ex)  #TODO: Logging
            return None
