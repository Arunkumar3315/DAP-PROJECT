#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pymongo
from pymongo import MongoClient
import json
import pandas as pd
import pprint
from pandas.io.json import json_normalize
import numpy as np

# MONGODB CONNECTION

client=MongoClient('mongodb://localhost:27017')

#CREATE DATABASE IN MONGODB
population_db=client['population_database']

#CREATE COLLECTION IN MONGODB DATABASE
population_collection=population_db['population']


#INSERT JASON DATA INTO MONGODB DATABASE
try:
    with open("Project_dataset/us-population-urban-area.json") as f:
        file_data = json.load(f)
        population_collection.insert_many(file_data)
        
except:
    print("Failed to insert data!!!!")
finally:
    print("Task Completed!!")
    client.close() 

# READING DATA TO AN ARRAY AND CONVERT TO CSV FILE
try:
    array=list(file_data)
    keys= array[0].keys()
    csv=pd.DataFrame(array).to_csv("Project_dataset/us-population-urban-area.csv")
except:
    print("Failed to convert the data to CSV!!!")
finally:
    print("The conversion task has been completed!!")


#DATA CLEANING PROCESS

csv_df=pd.read_csv("Project_dataset/us-population-urban-area.csv")
csv_df = csv_df.dropna()
csv_df=csv_df.drop(columns='recordid')
csv_df=csv_df.drop(columns='record_timestamp')
csv_df=csv_df.drop(columns='_id')
csv_df=csv_df.drop(columns='datasetid')

csv_df[['geography','target_geo_id2','year','population']]= csv_df.fields.str.split(",",n=3,expand=True,)
csv_df=csv_df.drop(columns='target_geo_id2')
csv_df=csv_df.drop(columns='fields')
csv_df=csv_df.drop(columns='year')
csv_df[['target_geo_id','population']]= csv_df.population.str.split(",",n=1,expand=True,)
csv_df[['target','population']]= csv_df.population.str.split(",",n=1,expand=True,)
csv_df=csv_df.drop(columns='target')
csv_df[['extra','target_geo_id']]= csv_df.population.str.split(":",n=1,expand=True,)
csv_df[['extra','population']]= csv_df.population.str.split(":",n=1,expand=True,)
csv_df=csv_df.drop(columns='extra')
csv_df[['target_geo_id','extra']]= csv_df.population.str.split("}",n=1,expand=True,)
csv_df=csv_df.drop(columns='extra')
csv_df[['population','extra']]= csv_df.population.str.split("}",n=1,expand=True,)
csv_df=csv_df.drop(columns='extra')
csv_df[['extra','geography']]= csv_df.geography.str.split(":",n=1,expand=True,)
csv_df=csv_df.drop(columns='extra')
csv_df[['extra','geography']]= csv_df.geography.str.split("'",n=1,expand=True,)
csv_df=csv_df.drop(columns='extra')
csv_df= csv_df.rename(columns={"Unnamed: 0": "P_id","population":"population","target_geo_id":"geo_id"})
csv_df.head(20)
csv_df = csv_df[csv_df.geography != "In micropolitan statistical area'"]
csv_df = csv_df[csv_df.geography != "United States'"]
csv_df = csv_df[csv_df.geography != "Puerto Rico'"]
csv_df = csv_df[csv_df.geography != "In metropolitan statistical area'"]

# CLEANED CSV FILE 
csv_df.to_csv(r'Project_dataset/arathy_final_output.csv', index = False)


# In[32]:


#POSTGRESQL CONNECTION
import psycopg2
import csv
db_Con=psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="1234")
db_Cursor=db_Con.cursor()

#CREATING A TABLE IN POSTGRESQL

try:
    db_Cursor.execute("""CREATE TABLE tbl_population
    (P_id text PRIMARY KEY,
    geography text,
    population text,
    geo_id text
    )""")
    print("Table created ")
except:
    print("error in creating table")
finally:
    print("task of creating a table completed")

#INSERTNG VALUES INTO TABLE

try:
  with open('Project_dataset/arathy_final_output.csv','r') as f:
         data_reader = csv.data_reader(f)
         next(data_reader) # Skip the header row.
         for row in data_reader:
            print(row)
            db_Cursor.execute("INSERT INTO tbl_population(P_id,geography,population,geo_id) VALUES (%s,%s,%s,%s)",(row))
            print("values inserted")
except:
    print("error in inserting values")
finally:
    print("insertion task completed")

db_Con.commit()
db_Cursor.close()
db_Con.close()


# In[22]:


# RETRIEVE STORED DATA FROM POSTGRESQL
import psycopg2

connection = psycopg2.connect( host="localhost",
                database="postgres",
                user="postgres",
                password="1234")
cursor = connection.cursor()
postgreSQL_select_Query = "SELECT * FROM tbl_population"
cursor.execute(postgreSQL_select_Query)
for table in cursor.fetchall():
    print(table)        
if(connection):
        cursor.close()
        connection.close()


# In[1]:


# BAR CHART VISUALIZATION

import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import plotly.express as px


connection = pg.connect( host="localhost",
                database="postgres",
                user="postgres",
                password="1234")
my_table    = pd.read_sql('SELECT * FROM tbl_population', connection)
df=pd.DataFrame(my_table)
fig = px.bar(df, x='geography', y='population',title='Population in the Metropolitan Areas of the United States')
fig.show()


# In[2]:


# SCATTER VISUALIZATION 


import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import plotly.express as px

connection = pg.connect( host="localhost",
                database="postgres",
                user="postgres",
                password="1234")
my_table    = pd.read_sql('SELECT * FROM tbl_population', connection)
df=pd.DataFrame(my_table)
fig = px.scatter(df, x='geography', y='population',title='Population in the Metropolitan Areas of the United States')
fig.show()


# In[ ]:




