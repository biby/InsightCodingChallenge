from dataFrame import DataFrame
from borderCrossing import datetype
from borderCrossing import partialaverage

df = DataFrame()
df.readCsvFile('Border_Crossing_Entry_Data_Full.csv',collumnsToRead=['Border','Measure','Date','Value'])
df.castType('Value',int)
df.castType('Date', datetype)

print(df)
df2 = df.agregate('Value')
print(df2)
#df2.sort(['Date','Value','Measure','Border'],False)

dic = df2.groupBy(['Measure','Border'],df2.collumns)
finaldf = DataFrame(df2.collumns+['Average'])
for _, df in dic.items():
    df.sort(['Date'])
    partaverage = partialaverage(df.getCollumn('Value'))
    df = df.addCollumn('Average', newCollumn=partaverage)
    finaldf.addDataFrame(df)

finaldf.sort(['Date','Value','Measure','Border'],False)
print(finaldf)
print(len(finaldf))

dateFormat = '%m/%d/%Y %I:%M:%S %p'
