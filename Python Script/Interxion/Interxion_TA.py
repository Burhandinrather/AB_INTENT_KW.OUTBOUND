from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import datetime
# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE
 

engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='AB_INTENT_KW',
    schema='OUTBOUND',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

# read csv data from source file

source_file_loc = (r"C:\Work\Audience Bridge\Round 2\Source\Interxion\Interxion TA List Data - Master.csv")
ext_file_location = (r"C:\Work\Audience Bridge\Round 2\Imported\Interxion\Interxion TA List Data - Master_updated.csv")

# if you want to read data from csv then uncomment the below line
df = pd.read_csv(source_file_loc,encoding ='latin1')

# if you want to read data from excel file then uncomment the below line
# df = pd.read_excel(source_file_loc)

# function to remove extra space between strings from columns of object data type
def whitespace_remover(dataframe):
   
    for i in dataframe.columns:
        if dataframe[i].dtype != 'object':
            pass
        else:
            dataframe[i] = dataframe[i].str.replace('\s+', ' ', regex=True)
whitespace_remover(df)

# create csv from the above data frame

df.to_csv(ext_file_location, encoding='utf-8',  index = False, sep=',')


# read csv data

df = pd.read_csv(ext_file_location)
df.columns = ['COMPANY_NAME', 'COUNTRY', 'WEBSITE']


table_name = "interxion_ta_list"

# insert df into snowflake
print(df)
connection = engine.connect()
df.to_sql(table_name, engine, if_exists='replace', index=False, index_label=None, chunksize=None, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')