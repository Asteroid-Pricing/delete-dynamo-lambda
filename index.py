
import boto3
from string import Template
from time import sleep

RETRIES = 20

def get_dynamo():
    try:
        dynamo = boto3.client('dynamodb')
        return (True, dynamo, None)
    except Exception as e:
        return (False, None, e)

def get_table_status(dynamo, table_name):
    try:
        result = dynamo.describe_table(TableName=table_name)
        status = result['Table']['TableStatus']
        return (True, status, None)
    except dynamo.exceptions.ResourceNotFoundException as e:
        return (True, 'DOESNOTEXIST', None)
    except Exception as e:
        return (False, None, e)
        
def delete_table(dynamo, table_name):
    try:
        result = dynamo.delete_table(TableName=table_name)
        return (True, result, None)
    except Exception as e:
        return (False, None, e)

def wait_on_desired_status(dynamo, table_name, desired_status, max_time, current_time):
    timeout = 5
    while True:
        if current_time > max_time:
            s = Template('wait_on_desired_status, tableName: $tableName never became $desiredStatus, waited $currentTime of $maxTime')
            return (False, None, Exception(s.substitute(tableName=table_name, desiredStatus=desired_status, currentTime=current_time, maxTime=max_time)))

        ok, status, error = get_table_status(dynamo, table_name)
        if ok == False:
            return (False, None, error)

        if status == desired_status:
            return (True, status, None)

        print('Status ' + status + ' is not ' + desired_status + '. Trying attempt ' + str(current_time + 1) + ' of ' + str(max_time) + ' in 5 seconds...')
        sleep(timeout)
        current_time = current_time + 1
        continue

def attempt_delete_table(dynamo, table_name):
    ok, status, error = get_table_status(dynamo, table_name)
    if ok == False:
        print("get_table_status error:", error)
        return (False, None, error)

    if status == 'DOESNOTEXIST':
        print("no table, delete success")
        return (True, None, None)

    if status == 'ACTIVE':
        ok, delete_result, error = delete_table(dynamo, table_name)
        if ok == False:
            print("delete_table error:", error)
            return (False, None, error)
        
        ok, _, error = wait_on_desired_status(dynamo, table_name, 'DOESNOTEXIST', RETRIES, 0)
        if ok == False:
            print("wait_on_desired_status error:", error)
            return (False, None, error)

        return (True, None, None)

    if status == 'DELETING':
        ok, _, error = wait_on_desired_status(dynamo, table_name, 'DOESNOTEXIST', RETRIES, 0)
        if ok == False:
            print("wait_on_desired_status error:", error)
            return (False, None, error)

        return (True, None, None)

    return (False, None, Exception('Table is inaccessible to delete.'))


def handler(event):
    ok, dynamo, error = get_dynamo()
    if ok == False:
        print("Failed to get dynamo:", error)
        raise error
    
    ok, _, error = attempt_delete_table(dynamo, event['tableName'])
    if ok == False:
        raise error
    
    print("Done! event:", event)
    return event

if __name__ == "__main__":
   handler({'tableName': 'asteroids'})