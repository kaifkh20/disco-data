class RedisParser:
    class encode :
        def bulk_string(string):
            length = str(len(string))
            res = "$"+length+"\r\n"+string+"\r\n"
            return res
        
        def simple_string(string):
            res = "+"+string+"\r\n"
            return res
    class decode:

        def executeCommand(cmnd,lst):
            if(cmnd=='ECHO'):
                # print(lst[1])
                word = lst[1]     
                return RedisParser.encode.bulk_string(word)
            elif (cmnd=='PING'):
                res = RedisParser.encode.simple_string("PONG")
                return res
                    

        def decodeArrays(string):
                lst = string.split("\r\n")
                length = lst[0][1]
                actLength = len(lst)
                if(length==0): return
                cmnd = lst[2] 
                # print(cmnd,length)
                return RedisParser.decode.executeCommand(cmnd,lst[3-actLength::])
