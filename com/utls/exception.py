# -*- coding:UTF-8 -*- #

class MyError(Exception):
    pass
    
class HiveError(MyError):
    def __init__(self,message):
        self.value = message
    def __str__(self):
        return repr(self.value)
    
class SqoopError(MyError):
    def __init__(self,message):
        self.value = message
    def __str__(self):
        return repr(self.value)
    
class HadoopError(MyError):
    def __init__(self,message):
        self.value = message
    def __str__(self):
        return repr(self.value)