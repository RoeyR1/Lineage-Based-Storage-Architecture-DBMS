from LineageBasedStorageDBMS.table import Table, Record
from LineageBasedStorageDBMS.index import Index

class Query:
    def __init__(self, table):
        self.table = table
        self.caller = "insert"

    def version(self, caller):
        if self.caller != caller:
            self.caller = caller
            self.table.make_ver_copy()

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        self.version("delete")
        self.table.delete(primary_key)
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # Assuming the first column is the key for simplicity
        self.version("insert")
        self.table.write(columns)
        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # Assuming search_key_index is always the key column for simplicity
        records = self.table.read_records(search_key_index, search_key, projected_columns_index)
        return records
    

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        if self.table.versions and relative_version!=0:
            self.table.is_history = True
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]
            records = self.select(search_key, search_key_index, projected_columns_index)
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]
            self.table.is_history = False
        else:
            records = self.table.read_records(search_key_index, search_key, projected_columns_index)

        return records


    

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
        

    def update(self, primary_key, *columns):
        self.version("update")
        columns = list(columns)
        if not columns[self.table.key_col]:
            columns[self.table.key_col] = primary_key
        if (not primary_key in self.table.page_directory[self.table.key_col]) or (not columns[self.table.key_col] == primary_key):
            return False
        self.table.update(columns)
        return True
        

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        for rid in range(start_range, end_range+1):
            value = self.table.read_value(aggregate_column_index, rid)
            sum += value if value else 0
        return sum

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        sum = self.sum(start_range,end_range,aggregate_column_index)
        if self.table.versions and relative_version!=0:
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]
            self.table.is_history = True
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]
            sum = self.sum(start_range, end_range, aggregate_column_index)
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]
            self.table.is_history = False
        return sum
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False