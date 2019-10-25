import datetime

dateFormat = '%m/%d/%Y %I:%M:%S %p'

class datetype():
    def __init__(self,stringDate):
        self.dt = datetime.datetime.strptime(stringDate,dateFormat)
        self = self.dt

    def __ge__(self,other):
        return self.dt.__ge__(other.dt)        

    def __le__(self,other):
        return self.dt.__le__(other.dt)

    def __gt__(self,other):
        return self.dt.__gt__(other.dt)

    def __lt__(self,other):
        return self.dt.__lt__(other.dt)
    

    def __eq__(self,other):
        return self.dt.__eq__(other.dt)

    def __hash__(self):
        return self.dt.__hash__()
    def __str__(self):
        return self.dt.strftime(dateFormat)
    
    


def partialaverage(collumn):
    partsums = [0]
    tot=0
    for i, val in enumerate(collumn[:-1]):
        tot+=val        
        partsums.append(round(tot/(i+1)+.0001))
    return partsums


