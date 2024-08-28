from LineageBasedStorageDBMS.table import Table, Record
from LineageBasedStorageDBMS.index import Index
import threading


class TransactionWorker:

    """
    # Creates transaction worker object.
    """

    def __init__(self, transactions=[]):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = None
        self.lock = threading.Lock()
        self.is_in_use = False

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """

    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    """
    Wait for the worker to finish
    """
    def join(self):
        if self.thread == None:
            return
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            while self.is_in_use:
                pass
            self.lock.acquire()
            self.stats.append(transaction.run())
            self.is_in_use = True
            self.lock.release()
            self.is_in_use = False
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
