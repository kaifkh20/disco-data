import os
import codecs
import time

time_limit = []
key_dict = {}

class RDB_PARSER:
    
             

        def __init__(self,directory,filename):
            self.directory = directory
            self.filename = filename

        def readDB(self):
            if os.path.exists(f'{self.directory}/{self.filename}'):
                f = open(f'{self.directory}/{self.filename}','rb')
                data = f.read()
                return data
            else:
                return b''
        
        def trimTheContent(self,data):
            if data==b'':
                return None
            res = data.hex(' ')
            idxofb = res.index('fb')
            idxoff = res.index('ff')
            # print("Raw data",res[idxofb:idxoff])
            lst = res[idxofb:idxoff].split('00')
            # print(lst)            
            reslst = []
            # i = 0
            for x in lst:
                reslst.append(str(x).strip().split(" "))
            
               
            return reslst
        def removeAllBlanks(self,data):
            return [value for value in data if value != ['']]

        def getTheHashSize(self,data):
            return data[0][1]

        def extractTheKeyValuePairs(self,data):
            global time_limit            
            if data==None:
                return None
            result = {}
            idx = -1
            # print("data reaching in key_value",data)
            for x in data:
                idx+=1
                if 'fb' in x:                    
                    continue
                if_fc=False                
                if 'fc' in data[idx-1]:
                    if_fc = True               
                
                lengthKey = int(x[0],16)
                l1 = x[1:lengthKey+1]
                l2 = x[lengthKey+1:len(x)-1]
                #print("this is l1 and l2",l1,l2) 
                if 'fc' not in x:
                    l2 = x[lengthKey+1:]
                key = ''
                value = ''
                value_integer = False
                for byt in l1:
                    key+=byt


                #print(l2)
                if 'c0' in l2[0]:
                    value_integer = True
                    value = int(l2[1],16)
                    break
                else:
                    
                    l2.pop(0)
                    for byt in l2:
                        value+=byt
                key = codecs.decode(key,'hex').decode('utf-8')
                
                if not value_integer:
                    value = codecs.decode(value,'hex').decode('utf-8')
                #print(key,value)
                
                if if_fc and len(time_limit)>0:
                    result[f"{key}"] = [f"{value}",time_limit.pop(0)]
                else:
                    result[f"{key}"] = f"{value}"
                
                    #result[f"{key}"] = f"{value}"
            return result
        def SeperateByFC(self,data):
            fc_size = 0
            #print("Data before seperation",data)
            global time_limit             
            try:
                fc_size = data[0][2]
            except:
                 fc_size = 0
            res_list = []
            idx = -1
            for x in data:
                 idx+=1
                 if idx>0 and 'fc' in data[idx-1]:
                        #fc_index = x.index('fc')
                        # res_list.append(x[:fc_index])
                        # res_list.append(x[fc_index:])
                        time_limit.append(x)
                 else:
                    res_list.append(x)  
          
            #print("This time limit",time_limit)
            #prinxt("This is fc seperated",res_list)
            res_time_list = []            
                        
            for x in time_limit:
                res = ''                
                for byt in reversed(x):
                    res+=byt
                res_time_list.append(int(res,16))
           
            time_limit = res_time_list

            return res_list
        
        def readAndStoreKey(self):
            global key_dict
            data = self.readDB()
            if data==b'':
                return None
            trimmedData = self.trimTheContent(data)
            trimmedData = self.removeAllBlanks(trimmedData)
            res_data = self.SeperateByFC(trimmedData)
            key_dict = self.extractTheKeyValuePairs(res_data)
            

        def getKeys(self):
            global key_dict
            # print("this is reaching getKeysFunction in RDBPARSER")
            # data = self.readDB()
            # if data==b'':
            #     return None
            # #print('the data is read',data)
            # trimmedData = self.trimTheContent(data)
            # #print('data trimmed',trimmedData)
            # trimmedData = self.removeAllBlanks(trimmedData)
            # #print('data trimmed',trimmedData)            
            # res_data = self.SeperateByFC(trimmedData)
            # #print("Data after fc seperation",res_data)                        
            # key_dict = self.extractTheKeyValuePairs(res_data)
            # print('key recieved',key_dict) 
            
            result = []
            print(key_dict)
            for k in key_dict:
                time_now = int(time.time())
                
                if type(key_dict[k]) is list:
                    key_time = "1"+str(key_dict[k][1])[:-1]
                    key_time = int(key_time)
                    if time_now<key_time:
                        result.append(k)
                else:
                    result.append(k)
            print(result)
            return result
        def getKeyByValue(self,key):
            global key_dict
            # data = self.readDB()
            # if data==b'':
            #     return None
            # trimmedData = self.trimTheContent(data)
            # res_data = self.removeAllBlanks(trimmedData)
            # res_data = self.SeperateByFC(res_data)
            # key_dict = self.extractTheKeyValuePairs(res_data)
            # print(res_data)
            print("key recieved",key_dict)
            if key in key_dict and type(key_dict[f"{key}"]) is list:
                key_time = "1"+str(key_dict[key][1])[:-1]
                #print(int(time.time()),int(key_time))
                key_time = int(key_time)
                if int(time.time()) < key_time:
                    return key_dict[key][0]
                
                return None
            else:
                return key_dict[key]
            

