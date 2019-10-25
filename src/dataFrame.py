from functools import reduce
class DataFrame:

    def __init__(self,collumns=None,dataFrames=None):
        if collumns!=None:
            self._collumns = collumns
        self.data=[]
        if dataFrames!=None:
            for df in dataFrames:
                self._addDataFrame(df)
        self._collumnType=None
        
    def _rowToStr(self,row):
        return ','.join(map(str,row))

    def addDataFrame(self,df):
        if df.nbCollumns!=self.nbCollumns:
            raise Exception('','')
        self.data.extend(df.data[:])



    def __str__(self):
        count=0
        printstring = self._rowToStr(self.collumns)+'\n'
        for row in self:
            if count>10:
                break
            count+=1
            printstring += self._rowToStr(row) + '\n'
        return printstring

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return self.data.__iter__()        
        
    @property
    def collumns(self):
        return self._collumns[:]
    
    @collumns.setter
    def collumns(self,collumnsName):    
        if self._collumns !=None and len(collumnsName)!=self.nbcollumns:
            raise Exception('','Lenghts do not match')
        self.data = tuple(collumnsName)

    @property
    def nbCollumns(self):
        return len(self._collumns)

    @property
    def nbRows(self):
        return len(self.data)

    @property
    def size(self):
        return (self.nbRows,self.nbCollumns)

    
    def getCollumn(self,collumn):
        col = self._indexifycollumn(collumn)
        return [row[col] for row in self]    
    

    def readCsvFile(self,filePath,delimiter=None,titleRow=True, collumnsToRead=None, collumnsToDrop=None, failSafe=False):
        '''
        Read a csv file and transform it to an array structure.
        
        Accept a list of collumns to read OR a list of collumns to drop.
        If a collumn listed doesn't exists, return an error, unless the failSafe flag is set to True
        
        
        '''
        self.data = []
        if delimiter == None:
            delimiter =','
        
        if collumnsToDrop != None and collumnsToRead != None:
            raise Exception('','')

        with open(filePath,'r') as csvFile:
            fileContent = csvFile.read()
        
        
        fileLines = fileContent.splitlines()
              
        nbCsvFileCollumns = len(fileLines[0].split(delimiter))
        
        self._collumns= None
        if titleRow:
            csvCollumns = fileLines.pop(0).split(delimiter)
            self._collumns = csvCollumns[:]
            
        #Deal with the collumnsToRead And collumnsToDrop, to get a list of collumns to keep as indices
        #Todo: Deal with the failSafe and remove dupplicate code
        if collumnsToRead != None:
            collumnFilter = lambda x : x
            collumnsList = collumnsToRead
        elif collumnsToDrop != None:
            collumnFilter = lambda x : not x
            collumnsList = collumnsToDrop
        if collumnsList:
            if all(isinstance(item, int) for item in collumnsToRead):
                collumnsToKeepIndices = [i for i in range(nbCsvFileCollumns) if collumnFilter(i in collumnsList)]
            elif all(isinstance(item, str) for item in collumnsList):
                if not titleRow:
                    raise Exception('Incompatible Input','')
                collumnsToKeepIndices = []
                self._collumns = []
                for i,c in enumerate(csvCollumns):
                    if collumnFilter(c in collumnsToRead):
                        collumnsToKeepIndices.append(i)
                        self._collumns.append(c)
        else:
            collumnsToKeepIndices = list(range(nbCsvFileCollumns))
        
        if self._collumns == None:
            self._collumns = list(map(lambda x: str(x), range(len(collumnsToKeepIndices))))
                


        #Extract rows one by one
        for line in fileLines:
            
            # Remove lines only containing spaces
            if line.strip()=='':
                continue
                            
            csvrow= list(map(lambda x: x.strip(), line.split(delimiter)))
            if len(csvrow)!=nbCsvFileCollumns:
                raise Exception('CSV format Error','')
            row = tuple([csvrow[i] for i in collumnsToKeepIndices]) 
            self.data.append(row)

    def writeCsvFile(self,filePath,titleRow=True, collumnsToWrite=None,collumnsToDrop=None, failSafe=False):
        '''
        Write the array in a csv file.

        Accept a list of collumns to write OR a list a collumns to drop.
        If a collumn listed doesn't exists, return an error, unless the failSafe flag is set to True.

        '''
        pass
    
    def _rowvalidity(self,row):
        if len(row)!=self.nbCollumns:
            return False
        return True

    def addRow(self,row):
        if not self._rowvalidity(row):
            raise Exception('Unvalid Row','')
        if self.data==None:
            self.data = []
        self.data.append(row)

    def _indexifycollumn(self, collumn):
        if isinstance(collumn,str):
            col = self.collumns.index(collumn)
        else:
            col = collumn
        return col

    def sumcollumn(self,collumnToSum,sumfunction=None):
        col = self._indexifycollumn(collumnToSum)
  
        if sumfunction==None:
            sumfunction = lambda x, y:x+y
        sumreduce = lambda x,y :sumfunction(x,y[col])
        initial = self.data[0][col]
        return reduce(sumreduce,self.data[1:],initial)
                        
    def _subRow(self,row, collumns):
        cols = [self._indexifycollumn(col) for col in collumns]
        return tuple(row[i] for i in cols)

    def groupBy(self,key,value=None):
        if value==None:
            value= self.collumns
        dataFrameDict={}
        for row in self:
            val = dataFrameDict.get(self._subRow(row,key),DataFrame(value))
            val.addRow(self._subRow(row,value))
            dataFrameDict[self._subRow(row,key)]=val
        return dataFrameDict


    def agregate(self,collumn,agregationFunction=None):
        col = self._indexifycollumn(collumn)
        
        if agregationFunction==None:
            agregationFunction = sum
        key =self.collumns
        key.pop(col)
        agregationmap = self.groupBy(key,[collumn])

        finaldf = DataFrame(self.collumns) 
        for key,val in agregationmap.items():
            agregatedValue = agregationFunction(val.getCollumn(collumn))
            finaldf.addRow(key[:col]+(agregatedValue,)+key[col:])
        return finaldf
            
    def castType(self,collumn,collumnType):
        col = self._indexifycollumn(collumn)
        for i,row in enumerate(self.data):       
            self.data[i]=row[:col]+(collumnType(row[col]),)+row[col+1:]


    def addCollumn(self,collumnName, collumnNumber=-1,defaultValue=None,newCollumn=None):
        if newCollumn!=None:
            if len(newCollumn) != self.nbRows:
                raise  Exception('','')
        elif defaultValue !=None:
            newCollumn = [defaultValue]*self.nbRows
        else:
            raise Exception('','')
        newCollumnNames = self.collumns
        if collumnNumber==-1:
            newCollumnNames.append(collumnName)
        else:
            newCollumnNames = newCollumnNames[:collumnNumber]+newCollumnNames[-1]+newCollumnNames[collumnNumber:-1]
        newDf = DataFrame(collumns=newCollumnNames)
        for row,newColVal in zip(self,newCollumn):
            newRow = row + (newColVal,)
            newRow = newRow[:collumnNumber]+(newColVal,)+newRow[collumnNumber:-1]
            newDf.addRow(newRow)
        return newDf
    
    def dropCollumn(self,collumnName):
        col = self._indexifyCollumn(collumnName)
        newCollumns = self.collumns
        newCollumns.pop(col)
        

    def sort(self,collumns, ascending=True):
        ''' 
        Sort the rows acording to the collumns list

        '''
        cols = list(map(self._indexifycollumn,collumns))
        
        def key(x):
            return tuple([x[i] for i in cols])
        self.data.sort(key=key,reverse = not ascending)
        
    
    def mapCollumn(collumnName, mappingFunction):
        colIndex = self.collumns.index(collumnName)
        newData = []
        for row in self.data:
            newData.append(row[:colIndex]+(mappingFunction(row[colIndex]),)+ row[colIndex+1:]) 
        self.data = newData       

    def filterRows(self,filterFunction):
        ''' 
        Return array composed of the rows on which the filterFunction is True

        '''
        newdf = DataFrame(self.collumns)
        for row in filter(filterFunction,self):
            newdf.addRow(row) 
        return newdf

    def dropRows(dropFunction):
        ''' 
        Return array composed of the rows on which the dropFunction is False

        '''
        return selectRows(lambda x: not dropFunction(x))

    
    
    
    
