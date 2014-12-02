import bsddb3 as bsddb
import output
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
            return False

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
            return False


    def retrieve_record_with_key_range(self, lower, upper):
        results = []
        #lower = int(lower)
        #upper = int(upper)
        #if (upper > DB_SIZE):
        #    upper = DB_SIZE

        #print(lower,upper)
        try:
            #assert(upper >= lower)
            for i in self.db_tree.keys():
                key = i.decode("utf-8")
                #key = i
                #print(key)
                #key = key.encode(encoding='UTF-8')
                if (key < upper and key > lower):

                    #print("hi")
                    #if (db[i] == data):
                    key = i
                    results.append((key,self.db_tree[key]))
                    #GIGA_PRINT(key,self.db_tree[key])
            return results
        except Exception as e:
            print (e)
