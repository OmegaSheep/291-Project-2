import bsddb3 as bsddb
import output
#directory = "/tmp/my_db"

DA_FILE1 = "/tmp/curnow_db/INDEX_DB_TREE"
DA_FILE2 = "/tmp/curnow_db/INDEX_DB_HASH"


def GIGA_PRINT(k, d, f=0):
    f = open('answers','a')
    f.write("\n")

    #key = i.decode("utf-8")
    #if f == 0:
    #if k
    f.write(k.decode('UTF-8')+"\n")
    f.write(d.decode('UTF-8')+"\n")
    #else:
    #f.write(k+"\n")
    #f.write(d+"\n")
    f.write("\n")
    f.close()

class index_file:

    def __init__(self, iterable):
        #Trys to open a database, if cant find, it will create it
        self.iterable = iterable
        print("Instantiating index file. . . [May take up to ~10 minutes for extremely large DB sizes]")
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
            key = key.encode(encoding='UTF-8')
            #print(key)
            #print(name)
            GIGA_PRINT(key,name)
            return results
        else:
            return False

    def retrieve_record_with_data(self, data):
        #self.data = data;
        results = []
        self.data = data.encode(encoding='UTF-8')
        name = ""
        try:
            name = self.db_hash[self.data]
            results.append(name)
            
        except:
            1+1
            #Do nothing! Nothing!

        if (results != None):
            #print("name:"+name)
            #print("data:"+data)
            data = data.encode(encoding='UTF-8')
            #name = name.encode(encoding='UTF-8')
            GIGA_PRINT(name,data)
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
