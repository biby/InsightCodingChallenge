
import sys
import datetime
import time
from dataFrame import DataFrame

dateFormat = '%m/%d/%Y %I:%M:%S %p'

class datetype():
    '''
        Date type
        encapsulates a datetime class.
    '''
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
    
    def monthDiff(self, date2):
        '''
            return the number of months between self and date2
        '''
        return 12*(self.dt.year - date2.dt.year) + (self.dt.month - date2.dt.month) 
    
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

def partialaverage(valueCollumn,dateCollumn=None):
    """
        Create the average collumn. 
        If no dateCollumn is provided compute the average with respect to the number of rows.
        If a date Collumn is provided compute the average with respect to the number of months that passed (considers missing months as value 0 months)
    """
    if len(valueCollumn)!=len(dateCollumn):
        raise Exceptions('The two list should have the same lenght')    
    if not valueCollumn:
        return []
    partialaverages = [0]
    
    tot = valueCollumn[0]
    if dateCollumn != None:
        monthDifferences = map(lambda date:date.monthDiff(dateCollumn[0]),dateCollumn[1:])
    else:
        monthDifferences = range(1,len(valueCollumn))

 
    for val,monthDifference in zip(valueCollumn[1:],monthDifferences):        
        partialaverages.append(roundHalfUp(tot/monthDifference))
        tot+=val        
    return partialaverages


if __name__=="__main__":
    if len(sys.argv)<3:
        raise Exception('Expecting input and output file paths')
    inputFilePath = sys.argv[1]
    outputFilePath = sys.argv[2]
    collumnsToRead = ['Border','Date','Measure','Value']


    df = DataFrame()
    df.readCsvFile(inputFilePath,collumnsToRead= collumnsToRead)
    df.castCollumnType('Value',int)
    df.castCollumnType('Date', datetype)

    df2 = df.agregate('Value')
    dic = df2.groupBy(['Measure','Border'],df2.collumns)
    finaldf = DataFrame(df2.collumns+['Average'])
    for df in dic.values():
        df.sort(['Date'])
        partaverage = partialaverage(df.getCollumn('Value'),df.getCollumn('Date'))
        #partaverage = partialaverage(df.getCollumn('Value'))
        df = df.addCollumn('Average', newCollumn=partaverage)
        
        finaldf.extend(df)

    finaldf.sort(collumns=['Date','Value','Measure','Border'],ascending=False)
    finaldf.writeCsvFile(outputFilePath)
    
    

