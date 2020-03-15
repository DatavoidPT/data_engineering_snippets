from src.lib.connectors.connector_jira import ConnectorJIRA
from src.lib.connectors.connector_aws_s3 import ConnectorS3
from src.lib.connectors.connector_file import ConnectorFile

from setup import configs


def run_all_issues_to_s3():
    # Initialize clients
    conn_s3 = ConnectorS3(bucket=configs.get('S3_BUCKET'))
    conn_jira = ConnectorJIRA(endpoint_url=configs.get('JIRA_ENDPOINT'),
                              username=configs.get('JIRA_USERNAME'),
                              password=configs.get('JIRA_PASSWORD'))
    conn_file = ConnectorFile(working_dir='../../../{}'.format(configs.get('STATIC_FOLDER')))

    # Get all issues
    jql_get_issues = 'project = Testing '
    issues_dict = conn_jira.execute_jql_todict(jql_get_issues)

    # Parse required fields
    processed_dict = [{
        "id": c['id'],
        "key": c['key'],
        "url": c['self'],
        "description": c['fields']['description'],
        "status": c['fields']['status']['name'],
        "summary": c['fields']['summary'],

    } for c in issues_dict['issues']]

    # Save to file
    if len(processed_dict) > 0:
        file_name = 'issues.csv'
        header = list(processed_dict[0].keys())
        file_path = conn_file.save_dict_to_csvfile(data=processed_dict
                                                   , file_name=file_name
                                                   , fieldnames=header)

        # Try copy
        conn_s3.upload_file(file_name=file_path
                            , bucket='eda0x7b2263-sbx-eu-west-1'
                            , destination_name='Uploads/{}'.format(file_name))

    # list all files
    for c in conn_s3.list_objects('Uploads'):
        print(c)