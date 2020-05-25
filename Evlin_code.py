#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pymongo
from pymongo import MongoClient
import json
import pandas as pd
import pprint
from pandas.io.json import json_normalize
import numpy as np
import psycopg2 as pg
from psycopg2.extras import execute_values
import plotly.express as px



###########ESTABLISHING CONNECTION WITH MONGODB############

db_client=MongoClient('mongodb://localhost:27017')
database=db_client['per_capita_income']
collection=database['pci_data']



try:
    with open("python_files/per_capita_income.json") as f:
        data = json.load(f)
        collection.insert_many(data)

except:
    print("error while inserting")
finally:
    print("insertion operation has been performed")
    db_client.close()


###############READING THE DATA TO AN ARRAY AND CONVERTING TO CSV###################


try:
    arr=list(data)
    keys = arr[0].keys()
    csv_file=pd.DataFrame(arr).to_csv("python_files/evlin_output.csv")

except:
    print("Failed to convert the data to CSV!!!")
finally:
    print("The conversion task has been completed!!")
    
    
#########################DATA CLEANING########################################



dataframe=pd.read_csv("python_files/evlin_output.csv")

dataframe = dataframe.dropna()
dataframe=dataframe.drop(columns='recordid')
dataframe=dataframe.drop(columns='record_timestamp')
dataframe=dataframe.drop(columns='_id')
dataframe=dataframe.drop(columns='datasetid')

dataframe[['region_code','region_name','year','fips_county_code','series_id','income']]= dataframe.fields.str.split(",",n=5,expand=True,)
dataframe=dataframe.drop(columns='fields')
dataframe.head(20)
dataframe=dataframe.drop(columns='year')
dataframe=dataframe.drop(columns='fips_county_code')
dataframe=dataframe.drop(columns='series_id')
dataframe.head(20)



dataframe[['incomeperyear','state_name']]= dataframe.income.str.split(",",n=1,expand=True,)
dataframe=dataframe.drop(columns='income')
dataframe=dataframe.drop(columns='state_name')
dataframe.head(20)



dataframe = dataframe[dataframe.region_name != " 'series_id': 'RPIPC29200'"]
dataframe = dataframe[dataframe.region_name != " 'series_id': 'RPIPC46520'"]
dataframe = dataframe[dataframe.region_name != " 'series_id': 'RPIPC48260'"]
dataframe = dataframe[dataframe.region_name != " 'series_id': 'RPIPC42200'"]
dataframe = dataframe[dataframe.region_name != " 'series_id': 'RPIPC31080'"]

dataframe = dataframe.replace(to_replace='2015:', value='')

dataframe[['year','per_capita_income']]= dataframe.incomeperyear.str.split(":",n=1,expand=True,)
dataframe=dataframe.drop(columns='incomeperyear')
dataframe=dataframe.drop(columns='year')

dataframe[['extra','region_name']]= dataframe.region_name.str.split(":",n=1,expand=True,)
dataframe=dataframe.drop(columns='extra')

dataframe[['extra','region_name']]= dataframe.region_name.str.split("'",n=1,expand=True,)
dataframe=dataframe.drop(columns='extra')


dataframe= dataframe.rename(columns={"Unnamed: 0": "Id","region_name":"region_name","per_capita_income":"per_capita_income"})
dataframe[['extra','region_code']]= dataframe.region_code.str.split(":",n=1,expand=True,)
dataframe=dataframe.drop(columns='extra')
dataframe[['extra','region_code']]= dataframe.region_code.str.split("'",n=1,expand=True,)
dataframe=dataframe.drop(columns='extra')
dataframe[['region_code','extra']]= dataframe.region_code.str.split("'",n=1,expand=True,)
dataframe=dataframe.drop(columns='extra')

dataframe.head(20)

###################CONVERTING CLEANED DATA TO CSV#########################

dataframe.to_csv(r'python_files/evlin_csv_output.csv', index = False)


##################ESTABLISHING POSTGRESQL CONNECTION AND CREATING DATABASE#################

import psycopg2

connection=psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="bridgit@1994")


cur=connection.cursor()

##############################CREATING A TABLE####################################################
try:
    cur.execute("""CREATE TABLE PCI_1
    (Id text PRIMARY KEY,
    region_code text,
    region_name text, 
    per_capita_income text
    )""")
except:
    print("Table Created!!!!!!!!")
finally:
    print("Task completed !!!!!!!!!!!")


###################################INSERTING THE CSV FILE AND STORING IN POSTGRESQL#########################

import csv


cur=connection.cursor()

try:
    with open('python_files/evlin_csv_output.csv', 'r') as f:
        reader = csv.reader(f)
        #print(reader)
        next(reader) # Skip the header row.
        for row in reader:
            print(row)
            cur.execute("INSERT INTO PCI_1(Id,region_code,region_name,per_capita_income) VALUES(%s,%s,%s,%s)",(row))
            print("values inserted")
except:
    print("Failed to insert values into the table!!!!!!!!!!!!!")
finally:
    print("Task Completed!!!!!!!!")
    
    
connection.commit()
cur.close()
connection.close()

#######################################READING DATA FROM POSTGRES#########################################

try:
    
    connection=psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="bridgit@1994")
    cur=connection.cursor()
    postgreSQL_select_Query = "SELECT * FROM pci_1"
    cur.execute(postgreSQL_select_Query)
    for table in cur.fetchall():
        print(table)
    
except:
    
    print("error in reading values")

finally:
    
    if(connection):
        cur.close()
        connection.close()


##################################################DATA VISUALISATION###########################################

try:
    connection = pg.connect( host="localhost",
                database="postgres",
                user="postgres",
                password="bridgit@1994")
    table = pd.read_sql('SELECT * FROM pci_1', connection)
    new_dataframe=pd.DataFrame(table)
    bar = px.bar(df, x='region_name', y='per_capita_income')
    bar.show()

    hist=px.histogram(new_dataframe, x="per_capita_income")
    hist.show()

    scat = px.scatter(new_dataframe, x="region_name", y="per_capita_income")
    scat.show()
    
except:
    
    print("Visualisation Error!!!!!!!!")
    
finally:
    if(connection):
        cur.close()
        connection.close()

