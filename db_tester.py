from LineageBasedStorageDBMS.db import Database
from LineageBasedStorageDBMS.query import Query
from time import process_time
from random import choice, randrange
import shutil
import os

#Ensure a new LineageBasedDB file is created each time by deleting the previous/already existing one
def reset_tester():
    test_db = 'LineageBasedDB'
    try:
        shutil.rmtree(test_db, ignore_errors = True)
    except Exception as e:
        print(f"Error occured during deletion of {test_db}: {e}")
    


reset_tester()
total_time = process_time()

db = Database()
user_table = db.create_table('Users', 5, 0) #5 column table
query = Query(user_table)
keys = []

ins_time_0 = process_time()
for i in range(0, 10000):
    query.insert(10000000 + i, 90, 0, 0, 0)
    keys.append(10000000 + i)
ins_time_1 = process_time()

print("Inserting 10k user records took:  \t\t\t", ins_time_1 - ins_time_0)

update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

upd_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
upd_time_1 = process_time()
print("Updating 10k user records took:  \t\t\t", upd_time_1 - upd_time_0)

# Measuring Select Performance
sel_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1])
sel_time_1 = process_time()
print("Selecting 10k user records took:  \t\t\t", sel_time_1 - sel_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 10000000 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 user record batch took:\t\t\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(10000000 + i)
delete_time_1 = process_time()
print("Deleting 10k user records took:  \t\t\t", delete_time_1 - delete_time_0)

db.close()

print("Total operation test took: ", process_time()-total_time)