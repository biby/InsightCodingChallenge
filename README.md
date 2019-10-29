# InsightCodingChallenge

Coding challenge for Insight Data Engineering camp

This coding challenge is completed using python3

## INSTRUCTIONS: 
----------------
This challenge is to implement two features:

1. Read the Border_Crossing_Entry_Data file and extract the collumns [Border,Date,Measure,Value]
2. Agregate the data with same Border Date and Measure and add a previous month average collumn.
3. Write a report file with the new data.


My project contains two files:

## dataFrame.py
----------------

This file contains a dataFrame handling class similar to the dataFrame class of the pandas library.
The data is stored as a list of tuples.

It can read and write data from and to a csv file.
It contains collumn and row  handling methods, such as add and drop, casting collumn types, mapping collumns
It contains a general data frame methods such as groupBy, agregate, filter and sort.


## borderCrossing.py
----------------

It contains functions specific to the project:
* A datetype class that can cast the dates in the csv file.
* A partialaverage method that compute the partial average collumn out of the value collumn, and the date collumn
* A roundHalfUp method, that round a float to the nearest integer, rounding up the half integers.

It also  contains the algorithm flow for creating the correct report.csv file

The algorithm is the following:
* Fills up a dataFrame out of the input File, extracting the 4 wanted collumns.
* Casts the value collumn to int and the date collumn to dateType.
* Divides the data into subsets with identical Border and Measure.
* Sorts each dataFrame with respect to the date and add the average collumn.
* Merges all the data and sort it appropriatly.
* Write to output file.

## Comments:
----------------

For the computation of the average collumn, I made the following assumption:
If for a given Month, Border and Date there is no data, then the Value is assumed to be 0.
For example, for the following data:
```
Border, Date, Measure,Value
US-Canada,03/01/2019 12:00:00 AM,Train, 10
US-Canada,01/01/2019 12:00:00 AM,Train, 10
```
the result will be

```
Border, Date, Measure,Value,Average
US-Canada,03/01/2019 12:00:00 AM,Train,10,5
US-Canada,01/01/2019 12:00:00 AM,Train,10,0
```

In order to consider that the monthe of February should just be ignore, it is sufficient to comment the line
        partaverage = partialaverage(df.getCollumn('Value'),df.getCollumn('Date'))
and uncomment the line
        #partaverage = partialaverage(df.getCollumn('Value')),
in the file src/borderCrossing.py
Note that in this case test_missing_months will fail.
