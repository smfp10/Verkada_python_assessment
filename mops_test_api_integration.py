##!/usr/bin/env python

import json


# Import any libraries you need
import requests

## Do not edit
class VerkadaDB():
    def __init__(self):
        self._data = {}
        ## You may add to the class definition below this line
        self.__last_key = {}  # used to keep track of the highest key in each table

    @property
    def table_columns(self):
        return [
            'name',
            'email',
            'domain',
            'topLevelName',
            'age',
            'gender',
            'nationality'
        ]

    @property
    def column_types(self):
        return {
            'name': str,
            'email': str,
            'domain': str,
            'topLevelName': str,
            'age': int,
            'gender': str,
            'nationality': str
        }

    ## To-do: add class methods
    def addTable(self, tableName: str):
        """
        Creates new table in DB if it does not exist.

        :param tableName: name of table to be created
        """
        self._data[tableName] = self._data.get(tableName, dict())
        self.__last_key[tableName] = self.__last_key.get(tableName, 0)

    def addRow(self, tableName: str, rowData):
        """
        Inserts row in existing table in DB.\n
        Primary keys for each row are integers. The __last_key attribute is used to keep track of the highest key in the
        table.

        :param tableName: name of table in which row will be inserted
        :param rowData: dictionary with data to be added to DB
        """

        # Raise error if specified table does not exist
        if tableName not in self._data.keys():
            raise ValueError("Table does not exist")
        # Generate row in table
        self.__last_key[tableName] += 1
        self._data[tableName][self.__last_key[tableName]] = self._data[tableName].get(self.__last_key[tableName], {})
        # If a row with the specified index already exist, change index and check again until it does not exist
        # Otherwise, continue
        while self._data[tableName][self.__last_key[tableName]]:
            self.__last_key[tableName] += 1
            self._data[tableName][self.__last_key[tableName]] = self._data[tableName].get(self.__last_key[tableName], {})
        # Add data to row
        for key in self.table_columns:
            if not isinstance(rowData.get(key, None), self.column_types[key]):
                raise ValueError(f"New {key} value is not of the correct type")
            self._data[tableName][self.__last_key[tableName]][key] = rowData.get(key, None)

    def _getMatchingRows(self, tableName: str, matchingCriteria):
        """
        Returns primary keys of rows that match the specified criteria

        :param tableName: name of table from which rows will be retrieved
        :param matchingCriteria: dictionary with matching criteria. Key is the column name, value is a tuple with the comparison operator and the value (e.g., {'age': ('>=', 30)})
        :return: list of primary keys for rows that match the specified criteria
        """

        # Validate method inputs
        # Raise error if specified table does not exist
        if tableName not in self._data.keys():
            raise ValueError("Table does not exist")
        # raise error if any key in matchingCriteria is not valid
        for key in matchingCriteria.keys():
            if key not in self.table_columns:
                raise ValueError(f"{key} is not a valid key")

        # Auxiliary function
        def _compare(row, k, criteria):
            if criteria[0] == "==":
                return row[k] == criteria[1]
            elif criteria[0] == ">":
                return row[k] > criteria[1]
            elif criteria[0] == "<":
                return row[k] < criteria[1]
            elif criteria[0] == ">=":
                return row[k] >= criteria[1]
            elif criteria[0] == "<=":
                return row[k] <= criteria[1]

        # Retrieve row keys with matching criteria
        matching_rows = [key for key, value in self._data[tableName].items()
                         if all(_compare(value, k, v) for k, v in matchingCriteria.items())]
        return matching_rows

    def getRows(self, tableName: str, matchingCriteria, limit: int = None):
        """
        Gets rows matching a certain criteria.

        :param tableName: name of table from which rows will be retrieved
        :param matchingCriteria: dictionary with matching criteria. Key is the column name, value is a tuple with the comparison operator and the value (e.g., {'age': ('>=', 30)})
        :param limit: maximum number of matches
        :return: dictionary with rows that match the specified criteria
        """

        # Get matching rows
        row_keys = self._getMatchingRows(tableName, matchingCriteria)
        if limit is not None:
            row_keys = row_keys[:limit]
        return {k: self._data[tableName][k] for k in row_keys}

    def updateRows(self, tableName: str, matchingCriteria, updateInformation):
        """
        Update information in rows matching the specified criteria

        :param tableName: name of table
        :param matchingCriteria: dictionary with matching criteria. Key is the column name, value is a tuple with the comparison operator and the value (e.g., {'age': ('>=', 30)})
        :param updateInformation: dictionary with new information
        """

        # Validate method inputs - raise error if any key in updateInformation is not valid
        # Remaining checks are in the _getMatchingRows function
        for key in updateInformation.keys():
            if key not in self.table_columns:
                raise ValueError(f"{key} is not a valid key")

        # Get the row numbers for rows with matching criteria
        row_keys = self._getMatchingRows(tableName, matchingCriteria)

        # Update info for matching rows
        for key in row_keys:
            for key_new, value_new in updateInformation.items():
                if type(value_new) is not self.column_types[key_new]:
                    raise ValueError(f"New {key_new} value ({type(value_new)}) is not of the correct type (should "
                                     f"be {self.column_types[key_new]})")
                self._data[tableName][key][key_new] = value_new

    def deleteRows(self, tableName, matchingCriteria):
        """
        Delete rows matching the specified criteria

        :param tableName: name of table
        :param matchingCriteria: dictionary with matching criteria. Key is the column name, value is a tuple with the comparison operator and the value (e.g., {'age': ('>=', 30)})
        """

        # Get the row numbers for rows with matching criteria
        row_keys = self._getMatchingRows(tableName, matchingCriteria)

        # Delete matching rows
        for key in row_keys:
            del self._data[tableName][key]


## Do not edit
dbInstance = VerkadaDB()

# Endpoint
endpoint = 'https://rwph529xx9.execute-api.us-west-1.amazonaws.com/prod/pushToSlack'


## To-do: Implement Function (mimics AWS Lambda handler)
## Input: JSON String which mimics AWS Lambda input
def lambda_handler(json_input):
    global dbInstance

    # Get email from input and stop function if it doesn't exist
    email = json.loads(json_input).get('email', None)
    if email is None:
        return

    # variable to store data from current email
    processed_input = dict()

    # Process email into separate parts
    processed_input['name'] = email.split('@')[0]
    processed_input['email'] = email
    processed_input['domain'] = email.split('@')[1].split('.')[0]
    if processed_input['domain'].casefold() == 'verkada':
        # Do not add people with a verkada domain to the table
        return
    processed_input['topLevelName'] = email.split(processed_input['domain'] + '.')[1]

    # Get age from API
    age = requests.get('https://api.agify.io?name=' + processed_input['name']).json()['age']
    processed_input['age'] = age

    # Get gender from API
    gender = requests.get('https://api.genderize.io?name=' + processed_input['name']).json()['gender']
    processed_input['gender'] = gender

    # Get nationality from API
    nationality = requests.get('https://api.nationalize.io?name=' + processed_input['name']).json()['country'][0]['country_id']
    processed_input['nationality'] = nationality

    # Store data in database
    dbInstance.addRow('Table1', processed_input)

    json_output = json.dumps(processed_input)
    ## Output: JSON String which mimics AWS Lambda Output
    response = requests.post(endpoint, json=json_output)  # Send to endpoint
    if response.status_code != 200:
        raise RuntimeError(f'POST failed for {email}')
    return json_output


## To Do: Create a table to hold the information you process
dbInstance.addTable('Table1')


## Do not edit
lambda_handler(json.dumps({"email": "John@acompany.com"}))
lambda_handler(json.dumps({"email": "Willy@bcompany.org"}))
lambda_handler(json.dumps({"email": "Kyle@ccompany.com"}))
lambda_handler(json.dumps({"email": "Georgie@dcompany.net"}))
lambda_handler(json.dumps({"email": "Karen@eschool.edu"}))
lambda_handler(json.dumps({"email": "Annie@usa.gov"}))
lambda_handler(json.dumps({"email": "Elvira@fcompay.org"}))
lambda_handler(json.dumps({"email": "Juan@gschool.edu"}))
lambda_handler(json.dumps({"email": "Julie@hcompany.com"}))
lambda_handler(json.dumps({"email": "Pierre@ischool.edu"}))
lambda_handler(json.dumps({"email": "Ellen@canada.gov"}))
lambda_handler(json.dumps({"email": "Craig@jcompany.org"}))
lambda_handler(json.dumps({"email": "Juan@kcompany.net"}))
lambda_handler(json.dumps({"email": "Jack@verkada.com"}))
lambda_handler(json.dumps({"email": "Jason@verkada.com"}))
lambda_handler(json.dumps({"email": "Billy@verkada.com"}))
lambda_handler(json.dumps({"email": "Brent@verkada.com"}))


## Put code for Part 2 here
dbInstance.updateRows(tableName='Table1',
                      matchingCriteria={'name': ('==', 'Kyle')},
                      updateInformation={'age': 26})

dbInstance.deleteRows(tableName='Table1',
                      matchingCriteria={'name': ('==', 'Craig')})

query_result = dbInstance.getRows(tableName='Table1',
                                  matchingCriteria={'age': ('>=', 30), 'gender': ('==', 'male')},
                                  limit=4)

# Output results as JSON
final_result = {
    'name': 'Simao Manuel Fernandes Pereira',
    'queryData': json.dumps(query_result),
    'databaseContents': json.dumps(dbInstance._data)
}
response = requests.post(endpoint, json=final_result)  # Send to endpoint
if response.status_code != 200:
    raise RuntimeError(f'POST failed for final result')
