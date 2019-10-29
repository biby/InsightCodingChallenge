
class DataFrame:

    def __init__(self,collumns=None,data=None):
        self._collumns=None        
        self.data=[]
        if collumns!=None:
            self._collumns = collumns
        
        if data!=None:
            self.extend(data)
        
    
    # Iteration tools
    def extend(self,iterable):
        '''
            Append an iterable collection of rows to the rows of self
        '''
        for row in iterable:
            self.addRow(row)
    
    def __len__(self):

        return len(self.data)

    def __iter__(self):
        '''
            Iteration on the rows
        '''
        return self.data.__iter__()



    #Printing tools

    def _rowToStr(self,row):
        '''
            Return a formated string reprensenting the row.
        '''
        return ','.join(map(str,row))

    def __str__(self,printHeader=True, MAXROWS=10):
        '''
            Return a printable display of the dataFrame, including the header and MAXROWS row
            If MAXROWS = -1 then print the whole dataFrame
        '''
        if MAXROWS==-1:
            MAXROWS = len(self)
        count=0
        header = ''
        if printHeader:
            header = self._rowToStr(self.collumns)+'\n'
        printstring = '\n'.join(map(self._rowToStr,self.data[:MAXROWS]))
        return header+printstring

            
    #Basic Properties  
    @property
    def collumns(self):
        return self._collumns[:]
    
    @collumns.setter
    def collumns(self,collumnsName):    
        if self._collumns !=None and len(collumnsName)!=self.nbCollumns:
            raise Exception('','Lenghts do not match')
        self._collumns = list(collumnsName)

    @property
    def nbCollumns(self):
        ''' 
            Number of collumns
        '''
        return len(self._collumns)

    @property
    def nbRows(self):
        '''
            Number of rows
        '''
        return len(self.data)

    @property
    def size(self):
        '''
            Tuple (Number of rows, Number of collumns)
        '''
        return (self.nbRows,self.nbCollumns)


    #Collumn tools
    
    

    def _indexifyCollumn(self, collumnName):
        '''
            Turn collumnName into its index.
            If the index is given, just returns it.
            
        ''' 
        if isinstance(collumnName,str):
            col = self.collumns.index(collumnName)
        elif isinstance(collumnName,int):
            col = collumnName
        else:
            raise Exception('')
        return col  
    
    def castCollumnType(self,collumnName,collumnType):
        '''
            Casts the elements of collumn to the type collumnType
        '''
        self.mapCollumn(collumnName, collumnType, inPlace=True)

    def getCollumn(self,collumn):
        ''' 
        Return the collumn "collumn" of the dataframe as a list
        ''' 
        col = self._indexifyCollumn(collumn)
        return [row[col] for row in self]

    def addCollumn(self,collumnName, collumnNumber=-1,defaultValue=None,newCollumn=None, inPlace = False):
        '''
            Return a dataFrame with an additional collumn. 
            The new collumn is either given as newCollumn or filled up with defaultValue
        '''

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
            newCollumnNames.insert(collumnNumber,collumnName)
        newDf = DataFrame(collumns=newCollumnNames)
        for row,newColVal in zip(self,newCollumn):
            newRow = row + (newColVal,)
            newRow = newRow[:collumnNumber]+(newColVal,)+newRow[collumnNumber:-1]
            newDf.addRow(newRow)
        if inPlace:
            self.__init__(newDf.collumns,newDf)
        else:
            return newDf

    def subDataFrame(self, newCollumns,inPlace=True):
        '''
            Extract a subdataframes with the collumns being newCollumns.
        ''' 
        
        newDf = DataFrame(newCollumns)
        
        newRows = map(lambda row: self._subRow(row,newDf.collumns),self)

        newDf.extend(newRows)
        if inPlace:
            self._collumns = newDf.collumns
            self.data = newDf.data
        else:
            return newDf
        
        
    def dropCollumns(self,collumnNames,inPlace=True):
        '''
            Drops the collumns collumnNames.
        '''
        cols = [self._indexifyCollumn(collumnName) for collumnName in collumnNames]
        newCollumns = [collumn for i,collumn in enumerate(self.collumns) if i not in cols]
        newDf = self.subDataFrame(newCollumns,inPlace=False)
        if inPlace:
            self._collumns = newDf.collumns
            self.data = newDf.data
        else:
            return newDf
    
    def mapCollumn(self,collumnName, mappingFunction, inPlace=True):
        '''
            Map the collumn collumnName through the function mappingFunction
        '''
        col = self._indexifyCollumn(collumnName)
        newData = []
        for row in self.data:
            newData.append(row[:col]+(mappingFunction(row[col]),)+ row[col+1:]) 

        if inPlace:        
            self.data = newData
        else:
            df = dataFrame(self.collumns)
            df.data = newData
            return df


    #Row tools

    def _rowvalidity(self,row):
        '''
            Checks if the format of the given Rows matches the one of the dataFrame
        '''
        if len(row)!=self.nbCollumns:
            return False
        return True

    def _subRow(self,row, collumns):
        '''
            Extract a subRow from row corresponding to collumns
        '''
        cols = [self._indexifyCollumn(col) for col in collumns]
        return tuple(row[i] for i in cols)
  
    def popRow(index=-1):
        '''
            Remove and return row at index (default last).
    
            Raises IndexError if dataFrame is empty or index is out of range.
        '''
        return self.data.pop(index)

    def getRow(index):
        return self.data[index]

    def filterRows(self,filterFunction):
        ''' 
        Return array composed of the rows on which the filterFunction is True

        '''
        newdf = DataFrame(self.collumns)
        for row in filter(filterFunction,self):
            newdf.addRow(row) 
        return newdf

    def addRow(self,row,index=None):
        '''
            Append a row to the dataFrame at index
        '''
        if not self._rowvalidity(row):
            raise Exception('Unvalid Row','')
        if index==None:
            self.data.append(row)
        else:
            self.data.insert(index,row)

    def dropRows(self,dropFunction):
        ''' 
        Return array composed of the rows on which the dropFunction is False
        '''
        return self.filterRows(lambda x: not dropFunction(x))
    

    #I/O tools

    def _fileToLines(self,fileContent):
        '''
            Takesthe fileContent and cleans it and  return a list of lines
        '''
        return list(filter(lambda line: line!='', fileContent.splitlines()))

    
    
    def _formatRow(self,line):
        row= tuple(map(lambda x: x.strip(), line.split(self._delimiter)))
        if not self._rowvalidity(row):
                raise Exception('CSV format Error','')
        return row

    def _extractData(self,fileLines):
        return list(map(lambda line : self._formatRow(line),fileLines))

    def readCsvFile(self,filePath,delimiter=None,titleRow=True, collumnsToRead=None, collumnsToDrop=None):
        '''
        Read a csv file and dataFrame.
        
        Accept a list of collumns to read OR a list of collumns to drop.
        If a list of collumns to read is given, then the order of the collumns in the dataFrame will be the same as in collumnsToRead
        '''
        self.data = []
        if delimiter == None:
            delimiter =','
        self._delimiter = delimiter
        
        if collumnsToDrop != None and collumnsToRead != None:
            raise Exception('','')

        with open(filePath,'r') as csvFile:
            fileContent = csvFile.read()

        
        #  Extract lines removing empty ones
        fileLines = self._fileToLines(fileContent)
        
        if not fileLines:
            raise Exception('Empty File')
        nbCsvFileCollumns = len(fileLines[0].split(delimiter))
        
        if titleRow:
            csvCollumns = fileLines.pop(0).split(delimiter)
            self.collumns = csvCollumns[:]
        else:
            self.collumns = list(map(lambda x: str(x), range(nbCsvFileCollumns)))
        
        #Extract lines
        self.data = self._extractData(fileLines)
        
        if collumnsToRead != None:
            self.subDataFrame(collumnsToRead,inPlace=True)          
        elif collumnsToDrop != None:
            self.dropCollumns(collumnsToDrop,inPlace=True)
        
        
        
    def writeCsvFile(self,filePath,titleRow=True, collumnsToWrite=None,collumnsToDrop=None, failSafe=False):
        '''
        Write the array in a csv file.

        Accept a list of collumns to write OR a list a collumns to drop.
        If a collumn listed doesn't exists, return an error, unless the failSafe flag is set to True.

        '''
        stringToWrite = self.__str__(printHeader=titleRow,MAXROWS=-1)

        with open(filePath,'w') as csvFile:
            csvFile.write(stringToWrite)
            
    #General DataFrame functions

    def groupBy(self,key,value=None):
        '''
            
            Splits the dataFrame in rows that match on the 'key' collumns.
            Return a dictionary of dataFrames with 'value' collumns 
            
            Key and value should be a list of collumns as names or indices.

            By default Value is all the collumns
        '''
        if value==None:
            value= self.collumns

        dataFrameDict={}
        
        for row in self:
            dataFrameDict.setdefault(self._subRow(row,key),DataFrame(value)).addRow(self._subRow(row,value))
        
        
        return dataFrameDict
        
    def agregate(self,aggregationCollumn,agregationFunction=None,inPlace=False):
        '''
            Returns a dataFrame where rows matching on all collumns but aggregationCollumn is replace by one single collumn.
            the value of aggregationCollumn for this row is defined by agregationFunction.

            The defaulf agregationFunction is sum

            if inPlace is True, then the agregation is done in place.
        '''
        col = self._indexifyCollumn(aggregationCollumn)
        
        if agregationFunction==None:
            agregationFunction = sum
        key =self.collumns
        key.pop(col)
        agregationmap = self.groupBy(key,[aggregationCollumn])

        finaldf = DataFrame(self.collumns) 
        for key,val in agregationmap.items():
            agregatedValue = agregationFunction(val.getCollumn(aggregationCollumn))
            finaldf.addRow(key[:col]+(agregatedValue,)+key[col:])
        if inPlace:
            self.data = finaldf.data
        else:        
            return finaldf

    def sort(self,collumns, ascending=True):
        ''' 
        Sort the rows acording to the collumns list

        '''
        cols = list(map(self._indexifyCollumn,collumns))
        
        def key(x):
            return tuple([x[i] for i in cols])
        self.data.sort(key=key,reverse = not ascending)
            



    
    
    
    
