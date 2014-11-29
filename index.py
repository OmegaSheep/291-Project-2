import bsddb3 as bsddb

directory = "/tmp/my_db"

DA_FILE1 = "/tmp/my_db/INDEX_DB_TREE"
DA_FILE2 = "/tmp/my_db/INDEX_DB_HASH"
DB_SIZE = 1000

class index_file:

    def __init__(self, iterable):
        #Trys to open a database, if cant find, it will create it
        try:
            self.db_tree = bsddb.btopen(DA_FILE1, "w")
        except:
            print("DB doesn't exist, creating a new one.")
            self.db_tree = bsddb.btopen(DA_FILE1, "c")
    
        try:
            self.db_hash = bsddb.hashopen(DA_FILE2, "w")
        except:
            print("DB doesn't exist, creating a new one.")
            self.db_hash = bsddb.hashopen(DA_FILE2, "c")

    def retrieve_record_with_key(self,key):
        self.db_tree[key]

    def retrieve_record_with_data():
        1+1

    def retrieve_record_with_key_range():
        1+1
            
