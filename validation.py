    
from pydash import (has, get, is_string)

class ValidationException(Exception):
    pass

def valid_table_name(event):
    result = get(event, 'tableName')
    if result is None:
        return ['Invalid tableName, none found in event.']

    if is_string(result) == False:
        return ['Invalid tableName, tableName is not a String.']

    if result == '':
        return ['Invalid tableName, tableName is blank.']

    return []

def valid_table_operation(event):
    result = get(event, 'tableOperation')
    if result is None:
        return ['Invalid tableOperation, none found in event.']

    if is_string(result) == False:
        return ['Invalid tableOperation, tableOperation is not a String.']

    lower_result = result.lower()
    print("lower_result:", lower_result)
    if lower_result != 'create' and lower_result != 'delete' and lower_result != 'deleteandcreate':
        return ['Invalid tableOperation, tableOperation is supposed to equal create, delete, or deleteandcreate.']

    return []

def valid_event(event):
    results = valid_table_name(event) + valid_table_operation(event)
    if len(results) > 0:
        return (False, None, ValidationException(' '.join(results)))
    return (True, event, None)