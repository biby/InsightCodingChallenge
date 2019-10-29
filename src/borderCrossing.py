
import sys
import datetime
import time
from dataFrame import DataFrame

dateFormat = '%m/%d/%Y %I:%M:%S %p'

class datetype():
    def __init__(self,stringDate):
        self.dateFormat=dateFormat
        self.dt = datetime.datetime.strptime(stringDate,self.dateFormat)
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
        return self.dt.strftime(self.dateFormat)
    
    
def roundHalfUp(n):
    '''
        Round a float to the nearest integers rounding halfs to the upper integers
    '''
    q = int(n)
    r=n%1
    if r >=.5:
        return q+1
    else: 
        return q

def partialaverage(collumn):
    if not collumn:
        return []
    partialaverages = [0]
    tot = collumn[0]
    for i, val in enumerate(collumn[1:]):
        partialaverages.append(roundHalfUp(tot/(i+1)))
        tot+=val        
    return partialaverages


if __name__=="__main__":
    if len(sys.argv)<3:
        raise Exception('Expecting input and output file paths')
    inputFilePath = sys.argv[1]
    outputFilePath = sys.argv[2]
    collumnsToRead = ['Border','Measure','Date','Value']


    df = DataFrame()
    df.readCsvFile(inputFilePath,collumnsToRead= collumnsToRead)
    df.castCollumnType('Value',int)
    df.castCollumnType('Date', datetype)

    df2 = df.agregate('Value')
    dic = df2.groupBy(['Measure','Border'],df2.collumns)
    finaldf = DataFrame(df2.collumns+['Average'])
    for df in dic.values():
        df.sort(['Date'])
        partaverage = partialaverage(df.getCollumn('Value'))
        df = df.addCollumn('Average', newCollumn=partaverage)
        
        finaldf.extend(df)

    finaldf.sort(collumns=['Date','Value','Measure','Border'],ascending=False)
    finaldf.writeCsvFile(outputFilePath)
    
    

