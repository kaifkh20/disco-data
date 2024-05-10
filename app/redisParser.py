class RedisParser:
    class encode :
        def bulk_string(string):
            length = len(string)
            res = "$"+length+"\r\n"+string+"\r\n"
            return res
        
        def simple_string(string):
            res = "+"+string+"\r\n"
            return res
    class decode:

        def executeCommand(cmnd,lst):
            if(cmnd=='ECHO'):
                  i = 0
                  for x in lst:
                    i+=1
                    if i%2==0:   
                        RedisParser().encode().bulk_string(x)
            elif (cmnd=='+PING'):
                res = RedisParser().encode().simple_string("PONG")
                return res
                    

        def decodeArrays(string):
                lst = string.split("\r\n")
                length = lst[0][1]
                actLength = len(lst)
                if(length==0): return
                if(length==2): RedisParser().decode().executeCommand(lst[0],[])
                cmnd = lst[2] 
                RedisParser().decode().executeCommand(cmnd,lst[3-actLength::])
