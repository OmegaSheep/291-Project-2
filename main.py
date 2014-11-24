# Berkeley DB Example

import sys, getopt

import bsddb3 as bsddb
import random
# Make sure you run "mkdir /tmp/my_db" first!
DA_FILE = "/tmp/my_db/sample_db"
DB_SIZE = 1000
SEED = 10000000

def get_random():
    return random.randint(0, 63)
def get_random_char():
    return chr(97 + random.randint(0, 25))

def generate_key():
    krng = 64 + get_random()
    key = ""
    for i in range(krng):
        key += str(get_random_char())

    key = key.encode(encoding='UTF-8')
    return key

def generate_value():
    vrng = 64 + get_random()
    value = ""
    for i in range(vrng):
        value += str(get_random_char())
    value = value.encode(encoding='UTF-8')
    return value

def retrieve_pair(db, key):
    try:
        Name = db[key]
        
    except:
        print (key + "Not exist!")
    if db.has_key(key) == True:
        name = db[key]
        return name

def main():
    
    arg = "\n".join(sys.argv)
    #print(arg)
    #gets last word of argument string (it should be hash or betree
    arg = arg.rsplit(None,1)[-1]

    #TODO actually use these args
    #argument handleing
    if arg == "btree" or arg == "1":
           #db_type_option = "btree"
        print("chosen btree")
        db_type_option = "btree" 
     
    elif arg == "hash" or arg == "2":
        print("choosen hash")
        db_type_option = "hash"  
    
    elif arg == "indexfile" or arg == "3" :
        print("choosen indexfile")
        db_type_option = "indexfile"
    else:
        print("please use the option btree, hash or indexfile")
        sys.exit()
       

    if db_type_option == "btree":
       
        #Trys to open a database, if cant find, it will create it
        try:
            db = bsddb.btopen(DA_FILE, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.btopen(DA_FILE, "c")
    #TODO fix unexpected filetype bug
    elif db_type_option == "hash":
        #Trys to open a database, if cant find, it will create it
        try:
            db = bsddb.hashopen(DA_FILE, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.hashopen(DA_FILE, "c")

    #TODO impliment this
    elif db_type_option == "indexfile":
        #Trys to open a database, if cant find, it will create it
        print("Impliment meeee!")
        try:
            db = bsddb.btopen(DA_FILE, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.btopen(DA_FILE, "c")
    else:
        print("LOL you shouldent be here")
        try:
            db = bsddb.btopen(DA_FILE, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.btopen(DA_FILE, "c")

    random.seed(SEED)

    #fills the database with stuff
    for index in range(DB_SIZE):
        key = generate_key()
        value = generate_value()

        #print (key)
        #print (value)
        #print ("")
        db[key] = value

    #inserts and finds a specific value
    test_key = "123"
    test_key = test_key.encode(encoding='UTF-8')
    db[test_key] = "found me!" 
    print(retrieve_pair(db, test_key))

    try:
        db.close()
    except Exception as e:
        print (e)

if __name__ == "__main__":
    main()
