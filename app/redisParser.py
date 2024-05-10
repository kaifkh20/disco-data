import json
import os
from threading import Timer
import string
import secrets

def create_random():
    alphabet = string.ascii_letters + string.digits
    random= ''.join(secrets.choice(alphabet) for i in range(40))
    return random

INFO = {
    "role" : "master",
    "master_replid":create_random(),
    "master_repl_offset":"0"
}

class RedisParser:
    class encode :

        def encode_array(string):
            length = len(string)
            res = "*"+str(length)+"\r\n"
            for x in string :
                lengthString = str(len(x))                
                res+="$"+lengthString+"\r\n"+x
            res+="\r\n"
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
            with open('data.json') as f:
                json_data = json.load(f)
            try :
                obj_val = json_data[val1]
                return RedisParser.encode.simple_string(obj_val)
            except:
                return RedisParser.encode.null()
        def executeCommand(cmnd,lst):
            print(cmnd,lst)
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

                return RedisParser.decode.executeSet(val1,val2,int(idxPXValue),pxValid)
            
            if(cmnd=='GET'):
                val1 = lst[1]
                
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

        def decodeArrays(string):
                lst = string.split("\r\n")
                length = lst[0][1]
                actLength = len(lst)
                if(length==0): return
                cmnd = lst[2] 
                # print(cmnd,length)
                return RedisParser.decode.executeCommand(str.upper(cmnd),lst[3-actLength::])
