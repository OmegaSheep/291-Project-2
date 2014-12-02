import bsddb3 as bsddb

#directory = "/tmp/my_db"

DA_FILE1 = "/tmp/curnow_db/INDEX_DB_TREE"
DA_FILE2 = "/tmp/curnow_db/INDEX_DB_HASH"

class index_file:

    def __init__(self, iterable):
        #Trys to open a database, if cant find, it will create it
        self.iterable = iterable
        print("Instantiating index file. . . ")
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

    def get_size(self):
        return self.iterable

    def retrieve_record_with_key(self,key):
        results = []
        self.key = key.encode(encoding='UTF-8')
        try:
            name = self.db_tree[self.key]
            results.append(name)
            
        except:
            1+1
            #Do nothing! Nothing!

        if (results != None):
            return results
        else:
            return

    def retrieve_record_with_data(self, data):
        #self.data = data;
        results = []
        self.data = data.encode(encoding='UTF-8')
        try:
            name = self.db_hash[self.data]
            results.append(name)
            
        except:
            1+1
            #Do nothing! Nothing!

        if (results != None):
            return results
        else:
            return


    def retrieve_record_with_key_range(self, lower, upper):
        results = []
        lower = int(lower)
        upper = int(upper)
        try:
            #assert(upper <= DB_SIZE)
            for i in range(lower, upper):
                self.key = str(i)
                self.key = self.key.encode(encoding='UTF-8')
                if (self.db_tree.has_key(self.key) == True):
                    #print("hi")
                    #if (db[i] == data):
                    results.append((self.key,self.db_tree[self.key]))
            return results
        except Exception as e:
            print (e)
            
