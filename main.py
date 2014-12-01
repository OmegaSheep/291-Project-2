# Berkeley DB Example

import sys, getopt
import os
import bsddb3 as bsddb
import random
import time
import index
#used for removing directorys
import shutil
#Makes directory if it does not exist.
directory = "/tmp/my_db"

if not os.path.exists(directory):
    os.makedirs(directory)
    #print(directory)
#Assignment spec files.
DA_FILE1 = "/tmp/my_db/DB_TREE"
DA_FILE2 = "/tmp/my_db/DB_HASH"
DA_FILE3 = "/tmp/my_db/DB_IndexFile"
#Test file.
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

def retrieve_pair_key(db, key):
    #print("hello")
    results = []
    key = key.encode(encoding='UTF-8')
    #key =
    try:
        #print (key,type(key))
        name = db[key]
        results.append(name)
        #return name
        
    except:
        print (key + ", does not exist!")

    return results
    #if db.has_key(key) == True:
    #    name = db[key]
    #    name = str(name)
    #    return name
    
def retrieve_pair_data(db, data):
    data = data.encode(encoding='UTF-8') 
    results = []
    try:
        for i in range(DB_SIZE):
            #print(i)
            key = str(i)
            key = key.encode(encoding='UTF-8') 
            if (db.has_key(key) == True):
                if (db[key] == data):
                    results.append((i,data))
                    1+1
        return results
    except Exception as e:
        print (e)


def retrieve_pair_range(db, lower, upper):
    results = []
    lower = int(lower)
    upper = int(upper)
    try:
        #assert(upper <= DB_SIZE)
        for i in range(lower, upper):
            key = str(i)
            key = key.encode(encoding='UTF-8')
            if (db.has_key(key) == True):
                #print("hi")
                #if (db[i] == data):
                results.append((key,db[key]))
        return results
    except Exception as e:
        print (e)

def main():
    
    arg = "\n".join(sys.argv)
    #print(arg)
    #gets last word of argument string (it should be hash or betree
    arg = arg.rsplit(None,1)[-1]

    #TODO actually use these args
    #argument handleing
    os.system('clear')
    if (arg == "btree" or arg == "1"):
           #db_type_option = "btree"
        print("Chosen DB_BTREE.")
        db_type_option = "btree" 
     
    elif (arg == "hash" or arg == "2"):
        print("Chosen DB_HASH.")
        db_type_option = "hash"  
    
    elif (arg == "indexfile" or arg == "3"):
        print("Chosen IndexFile.")
        db_type_option = "indexfile"
    else:
        print("Please use the option 'btree', 'hash' or 'indexfile'.")
        sys.exit()
       
    if db_type_option == "btree":
       
        #Trys to open a database, if cant find, it will create it
        try:
            db = bsddb.btopen(DA_FILE1, "w")
        except:
            print("DB doesn't exist, creating a new one.")
            db = bsddb.btopen(DA_FILE1, "c")
            
    elif db_type_option == "hash":
        try:
            db = bsddb.hashopen(DA_FILE2, "w")
        except:
            print("DB doesn't exist, creating a new one.")
            db = bsddb.hashopen(DA_FILE2, "c")
            
    elif db_type_option == "indexfile":
        try:
            db = bsddb.rnopen(DA_FILE3, "w")
        except:
            print("DB doesn't exist, creating a new one.")
            db = bsddb.rnopen(DA_FILE3, "c")
            
    else:
        print("LOL you shouldn't be here.")
        assert("You shall not pass." == potato)

 
    while (1):  
        print("Please press the corresponding number to select option.")
        print("[1]: Create and populate database")
        print("[2]: Retrieve a record with a given key value.") 
        print("[3]: Retrieve a list of records with a given data value.")
        print("[4]: Retrieve a list of records with a given range of key values.")
        print("[5]: Destroy Database")
        print("[6]: Exit. ")            
                  
        opt = str(input("Input: \n"))
        os.system('clear')

        if(opt == '1'):          
  
            if not os.path.exists(directory):
                os.makedirs(directory)
                #print(directory)
            random.seed(SEED)

            #Need to behave differently for indexFile
            if db_type_option != "indexfile":
                for j in range(DB_SIZE):
                    key = generate_key()
                    value = generate_value()
                    print (key)
                    print (value)
                    print ("")
                    db[key] = value
                    #   Don't do tests if using index!
                    #inserts and finds a specific value

                    test_key = "125"
                    test_key = test_key.encode(encoding='UTF-8')
                    db[test_key] = "found me!" 
                    #print(retrieve_pair_key(db, test_key))

                    test_key = "124"
                    test_key = test_key.encode(encoding='UTF-8')
                    db[test_key] = "found me!"
            else:
                theIndex = index.index_file(DB_SIZE)

        elif (opt == '2'):
            
            key = str(input("Please enter key value: \n"))
            try:
                t1 = time.clock()
                result = retrieve_pair_key(db, key)
                print("Result Found: "+str(result))
                print("Number of records found: " +str(len(result)))
                print("Time taken:",(time.clock() - t1)*1000000)
            except Exception as e:
                print(e)
                
        elif (opt == '3'):
            data = str(input("Please enter data value: \n"))
            #data = data.encode(encoding='UTF-8') 
            try:
                t2 = time.clock()
                result = retrieve_pair_data(db, data)
                print("Result List for Data Value:",data)
                print(result)
                print("Number of records found: " +str(len(result)))
                print("Time taken:",(time.clock() - t2)*1000000)
            except Exception as e:
                print(e)                
    
        elif (opt == '4'):
            lower = int(input("Please enter lower bound: \n"))
            upper = int(input("Please enter upper bound: \n"))
            assert(lower <= upper)
            try: 
                t3 = time.clock()
                result = retrieve_pair_range(db, lower, upper)
                print("Result Found:",result)
                print("Number of records found: " +str(len(result)))
                print("Time taken:",(time.clock() - t3)*1000000)
            except Exception as e:
                print(e)                      
                
        elif (opt =='5'):
            shutil.rmtree(directory)

            try:
                db.close()
            except Exception as e:
                print (e)

        #removes database and stuff
        elif (opt == '6'):
            shutil.rmtree(directory) 
            try:
                db.close()
            except Exception as e:
                print (e)

            sys.exit("Exiting database application. . .")
                
        else:
            os.system('clear')
            print("Sorry " + opt + " is not a known option. \n")   
                
    #inserts and finds a specific value
    #test_key = "123"
    #test_key = test_key.encode(encoding='UTF-8')
    #db[test_key] = "found me!" 
    #print(retrieve_pair_key(db, test_key))

    try:
        db.close()
    except Exception as e:
        print (e)

if __name__ == "__main__":
    main()
