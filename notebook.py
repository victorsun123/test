# Databricks notebook source
# DBTITLE 1,Import Class Library
# MAGIC 
# MAGIC %run ./Import_class_library

# COMMAND ----------

# DBTITLE 1,Declare Variables
NotebookStart = GetStepStart()
FileNm='CustomerProfile'

# Import Libraries for setting date variable
from datetime import date
import pandas as pd

#setFileCreationDate Variable
dt = pd.to_datetime(date.today(), format = '%Y-%m-%d')
FileCreateDate = dt.strftime('%Y%m%d')  #print(d.isoformat())

PathFileNm=spark.sql('''SELECT  (rtrim(DisplayValue) 
                               ||'/'||'{FileName}'
                               ||'_'||'{FileCreateDt}'
                               ||'.'||rtrim(lookupvalue2)
                               ) as FilePath 
                      FROM {DB}.ref_business_rules 
                      WHERE MappingRule = 'SalesforceExtract' 
                      AND LookupValue= '{FileName}' '''.format(DB = DatabaseName, FileName = FileNm, FileCreateDt=FileCreateDate)).toPandas()['FilePath']

# Build filepath + File Name variable
#PathFileNm= FilePath + '/' + FileNm + '_' + FileCreateDate + '.'
PathFileNm[0]

# //root/*.txt
# chkPrevFile=spark.sql('''SELECT  '*.'||rtrim(lookupvalue2)  as FilePath 
#                       FROM {DB}.ref_business_rules 
#                       WHERE MappingRule = 'SalesforceExtract' 
#                       AND LookupValue= '{FileName}' '''.format(DB = DatabaseName, FileName = FileNm, FileCreateDt=FileCreateDate)).toPandas()['FilePath']

# COMMAND ----------

#create dataframe with data to export
df = sql(''' Select Distinct 
                   MD.SAPCustomerNumber
                 , MD.BusinessPartnerName
                 , MD.StreetAddress
                 , MD.City
                 , MD.Region
                 , MD.CountryKey
                 , MD.PostalCode
                 , MD.FirstTelephoneNumber
                 , SA.DeliveringPlant
                 , MD.SAPSearchTerm1
FROM enterprise_data_model.dim_business_partner MD
LEFT JOIN enterprise_data_model.dim_business_partner_sales_area SA
ON MD.SAPCustomerNumber = SA.SAPCustomerNumber
WHERE SA.OrderBlockForSalesArea is null 
  AND MD.SAPCustomerAccountGroup = 'ZCPD' ''')  


#convert python dataframe to Pandas dataframe
pandas_df = df.toPandas()

# COMMAND ----------

# DBTITLE 1,Write JSON file to egress Location 
#pandas_df.to_json( PathFileNm , orient = 'split', compression = 'infer')
#pandas_df.to_json( PathFileNm[0] )
pandas_df.to_json( PathFileNm[0], orient = 'records', compression = 'infer')

 

# COMMAND ----------

# DBTITLE 1,Log Notebook Finish
NotebookEnd = GetStepStart()
#LogLoadStep(NotebookEnd, LoadID, TableName, 'Notebook Finish')
print(NotebookStart)
print(NotebookEnd)

# COMMAND ----------

FileNm
# importing the module
import pandas as pd

# reading the JSON file
df = pd.read_json( PathFileNm[0]  )

df.count()