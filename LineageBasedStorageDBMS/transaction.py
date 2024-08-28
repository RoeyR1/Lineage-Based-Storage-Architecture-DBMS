from LineageBasedStorageDBMS.table import Table, Record
from LineageBasedStorageDBMS.index import Index
import threading


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.is_in_use = False
        self.lock = threading.Lock()

    """
    # Adds the given query to this transaction
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))

        

    def run(self):
        for query, args in self.queries:
            while self.is_in_use:
                pass
            self.lock.acquire()
            result = query(*args)
            self.is_in_use = True
            # If the query has failed the transaction should abort
            self.lock.release()
            self.is_in_use = False
            if result == False:
                return False
        return True

