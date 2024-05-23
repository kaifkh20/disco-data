import json
import os
from threading import Timer
import string
import secrets
import base64
from .rdbParser import RDB_PARSER


EMPTY_RDB_FILE = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

def create_random():
    alphabet = string.ascii_letters + string.digits
    random= ''.join(secrets.choice(alphabet) for i in range(40))
    return random

INFO = {
    "role" : "master",
    "master_replid":create_random(),
    "master_repl_offset":"0"
}

BYTES_RECIEVED = 0

class RDB:
    DIR = ""
    DB_FILE_NAME = ""
    EXECUTE_RDB = False
    
class RedisReplica:
    NO_OF_REPLICAS = 0
    NO_OF_REPLICAS_ACK = 0

class RedisParser:
    class encode :


        def simple_error(err):
            return f"-{err}\r\n"

        def encode_integer(num):
            res = f":{num}\r\n"
            return res

        def encode_replica_bulk_string(cmnd,lst):

            res_lst = [cmnd]
            i = 1
            for x in lst:
                if i%2==0:
                    res_lst.append(x)
                i+=1
            return RedisParser.encode.encode_array(res_lst)


        def encode_rdb():
            content = base64.b64decode(EMPTY_RDB_FILE)
                # print(content)
            # res = "".join(["{:08b}".format(x) for x in content])
            # print("$"+str(content_size)+"\r\n"+res)
            content_size = len(content)
            return [f"${str(content_size)}\r\n",content]

        def encode_array(string):
            length = len(string)
            res = "*"+str(length)+"\r\n"
            for x in string :
                lengthString = str(len(x))                
                res+="$"+lengthString+"\r\n"+x+"\r\n"
            # res+="\r\n"
            return res
        
        def bulk_string(string):  
            res="$"
            if(type(string) is list):
                length = str(string[-1])
                string.pop()
                res+=length+"\r\n"
                for x in string:
                    res += x
                res+="\r\n"
            else:
                length = str(len(string))
                res += length+"\r\n"+string+"\r\n"
            return res
        
        def simple_string(string):
            res = "+"+string+"\r\n"
            return res

        def null():
            return "$-1\r\n"
    class decode:
            
        def executeKeys():
            print('This is reaching execute keys')
            result = RDB_PARSER(RDB.DIR,RDB.DB_FILE_NAME).getKeys()
            return result
            
        def executeWait(if_done):
            if_done[0] = True
            # return RedisParser.encode.encode_integer(num=RedisReplica.NO_OF_REPLICAS_ACK)


        def executeSet(val1,val2,pxValue,pxValid):
    
            def removeKey(delVal):
                # print("reaching")
                if os.path.exists('data.json'):
                    with open('data.json') as f:
                        json_data = json.load(f)
                        # print(json_data)
                        json_data.pop(delVal)
                    with open('data.json','w') as f:
                        json.dump(json_data,f)


            data = {val1:val2}

            if os.path.exists('data.json'):
                with open('data.json') as f:
                    json_data = json.load(f)
                    # print(json_data)
                    json_data.update(data)
                with open('data.json','w') as f:
                    json.dump(json_data,f)
            else :
                with open('data.json', 'w+') as f:
                    json.dump(data,f,ensure_ascii=False)

            if(pxValid):
                Timer(pxValue/1000,removeKey,(val1,)).start()
            return RedisParser.encode.simple_string("OK")

        def executeGet(val1):
            if RDB.EXECUTE_RDB:
                result = RDB_PARSER(RDB.DIR,RDB.DB_FILE_NAME).getKeyByValue(key=val1)
                if result==None:
                    return RedisParser.encode.null()
                return RedisParser.encode.bulk_string(result)

            with open('data.json') as f:
                json_data = json.load(f)
            try :
                obj_val = json_data[val1]
                if type(obj_val) is dict:
                    obj_val = obj_val["type"]
                return RedisParser.encode.simple_string(obj_val)
            except:
                return RedisParser.encode.null()
        def executeCommand(cmnd,lst,replica=False,bytes_recv=0):
            # print(cmnd,lst)
            global BYTES_RECIEVED
            if(cmnd=='ECHO'):
                # print(lst[1])
                word = lst[1]     
                return RedisParser.encode.bulk_string(word)
            
            if(cmnd=='SET') : 

                val1 = lst[1]
                val2 = lst[3]
                idxPXValue = 0
                pxValid = False
                if 'px' in lst or 'PX' in lst or 'pX' in lst:
                    idxPx = lst.index('px'or'PX'or'pX')
                    idxPXValue = lst[idxPx+2]
                    pxValid = True

                
                if replica:
                    # print(RedisParser.encode.encode_array(lst))
                    RedisParser.decode.executeSet(val1,val2,int(idxPXValue),pxValid)
                    return RedisParser.encode.encode_replica_bulk_string(cmnd,lst)

                
                return RedisParser.decode.executeSet(val1,val2,int(idxPXValue),pxValid)
            
            if(cmnd=='GET'):
                val1 = lst[1]

                if replica:
                    RedisParser.decode.executeGet(val1)
                    return RedisParser.encode.encode_array(['GET',val1])
                
                return RedisParser.decode.executeGet(val1)
            
            if(cmnd=='INFO'):
                header = lst[1]
                stringList = list()
                lengthString = 0
                for k,v in INFO.items():
                    key_value_pair=k+":"+v
                    lengthString+=len(key_value_pair)
                    stringList.append(key_value_pair)
                stringList.append(lengthString)

                return RedisParser.encode.bulk_string(stringList)


            if(cmnd=='PING'):
                
                return RedisParser.encode.simple_string("PONG")
            
            if(cmnd=='REPLCONF'):
                if 'GETACK' in lst:
                    BYTES_RECIEVED=bytes_recv
                    return RedisParser.encode.encode_array([cmnd,'ACK',str(BYTES_RECIEVED)])
                return RedisParser.encode.simple_string("OK")
            if(cmnd=='PSYNC'):
                return [RedisParser.encode.simple_string("FULLRESYNC "+INFO.get("master_replid")+" "+INFO.get("master_repl_offset")),RedisParser.encode.encode_rdb()]
            
            if(cmnd=='WAIT'):
                print("It's reaching here")
                rep = lst[1]
                time = int(lst[3])
                if_done = [False]
                matched = True
                timer = Timer(time/1000,function=RedisParser.decode.executeWait,args=(if_done,))
                timer.start()
                while not int(rep)<=RedisReplica.NO_OF_REPLICAS_ACK:
                    #print('reaching in loop')
                    if (if_done[0]==True):
                        matched = False
                        break
                #timer.join()
                print(RedisReplica.NO_OF_REPLICAS_ACK,"no of replica ack")
                if matched:
                    num = int(rep)
                else : num = RedisReplica.NO_OF_REPLICAS_ACK
                res = RedisParser.encode.encode_integer(num)
                RedisReplica.NO_OF_REPLICAS_ACK = 0
                return res
            if cmnd=='CONFIG':
                get = lst[1]
                value = lst[3]
                    
                if value=='dir':
                    return RedisParser.encode.encode_array(['dir',RDB.DIR])
                elif value=='dbfilename':
                    return RedisParser.encode.encode_array(['dbfilename'])
            
            if cmnd=='KEYS':
                key = lst[1]
                # print(key)
                print('this is reaching here')
                if key=="*":
                    res = RedisParser.decode.executeKeys()
                print('This is the key',lst)
                
                return RedisParser.encode.encode_array(res)

            if cmnd=="TYPE":
                val1 = lst[1]
                response = RedisParser.decode.executeGet(val1=val1)
                if(response==RedisParser.encode.null()):
                    return RedisParser.encode.simple_string("none")
                else:
                    if response!="+stream\r\n":
                        return RedisParser.encode.simple_string("string")
                    return response
            if cmnd == "XADD": 
                def validate_id(id,name):
                    with open('data.json') as f:
                        json_data = json.load(f)
                        if name not in json_data:
                            f.close()
                            return True
                        enteries = json_data[name]["enteries"]
                        for x in enteries:
                            if 'id' in x:
                                first_part = int(x['id'][0])
                                second_part = int(x['id'][2])
                                if x['id']==id or first_part>int(id[0]) or x['id']=='0-0'or (first_part==int(id[0]) and second_part>int(id[2])):
                                    f.close()
                                    return False
                    f.close()
                    return True
                cmnd_lst = []
                for x in lst:
                    if x != '' and x[0]!='$':
                        cmnd_lst.append(x)

                name = cmnd_lst.pop(0)
                id = cmnd_lst.pop(0)

                dict_key_value = {}

                if(id=="0-0"):
                    return RedisParser.encode.simple_error(err="ERR The ID specified in XADD must be greater than 0-0")

                if(not validate_id(id=id,name=name)):
                    return RedisParser.encode.simple_error(err="ERR The ID specified in XADD is equal or smaller than the target stream top item")
                
                i = 0
                while i<len(cmnd_lst)-1:
                    dict_key_value[f"{cmnd_lst[i]}"] = f"{cmnd_lst[i+1]}"
                    i+=2
                

                dict_key_value["id"] = id
                data = {name:{"type":"stream","enteries":[dict_key_value]}}

                with open('data.json') as f:
                    json_data = json.load(f)
                    if name in json_data:
                        json_data[name]["enteries"].append(dict_key_value)
                    else:
                        json_data.update(data)
                with open('data.json','w+') as f:
                    json.dump(json_data,f)

                return RedisParser.encode.bulk_string(id)
        def decodeSimpleString(string):
            lst = string.split("\r\n")
            lst = lst[0].split("+")
            return lst[1]

        def decodeOnlyCommand(string):
            lst = string.split("\r\n")
            #print(lst)
            if len(lst)<=1: return ""
            if len(lst)==2: return lst[0].split("+")[1]
            return lst[2]

        def decodeArrays(string,replica=False,bytes_recv=0):
                # print(string)
                lst = string.split("\r\n")
                actLength = len(lst)
                if lst == [''] : return 
                if(actLength==0): return
                # print(lst)
                cmnd = lst[2]
                # print(lst)
                   
                return RedisParser.decode.executeCommand(str.upper(cmnd),lst[3-actLength::],replica,bytes_recv)
