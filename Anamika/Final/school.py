


# In[1]:


import pandas as pd
import pyodbc
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


# In[2]:


svr_name = 'KU1ICDDWV011.kunet.ae'
db_name = 'CACM'
u_name = 'cacm_user'
u_pass = 'Welcome#123'


# In[3]:


eng = create_engine("mssql+pyodbc://"+u_name+":"+u_pass+"@"+svr_name+"/"+db_name+"?driver=ODBC+Driver+17+for+SQL+Server")


# In[4]:


query_payroll = "select * from ADERP.dbo.KU_Payroll_All"
query_education ="select * from ADERP.dbo.XXDOF_BI_KU_EDUCATION_T"
query_basic_info = "select * from ADERP.dbo.XXDOF_BI_KU_Emp_Basic_Info_T"
query_payroll_T =  "select * from ADERP.dbo.XXDOF_BI_KU_PAYROLL_T"


# In[5]:


payroll = pd.read_sql(query_payroll, eng)
education = pd.read_sql(query_education, eng)
info = pd.read_sql(query_basic_info, eng)
payroll_t = pd.read_sql(query_payroll_T, eng)


# In[6]:


child = education[['EMPLOYEE_NUMBER','CHILD_NAME']].drop_duplicates()
eligible = education[['EMPLOYEE_NUMBER','MAX_CHILD_ELIGIBLE']].drop_duplicates()
emp = info[['EMPLOYEENUMBER','NATIONALITY','MARITALSTAT']].drop_duplicates()


# In[7]:


def schooling_exceed():

    """This script will check if the education amount paid is more than ....."""

    education = pd.read_sql(query_education, eng)

    education = education.astype({'AMOUNT_TO_BE_PAID':'float'})

    ed_final = education[['EMPLOYEE_NUMBER','ACADEMIC_YEAR','ACADEMIC_SEM','CHILD_NAME','DATE_OF_BIRTH','TOTAL_ENTITLEMENT','CHILD_ENTITLEMENT_AMOUNT','SCHOOL','AMOUNT_TO_BE_PAID']]

    ed_final = ed_final[-education['ACADEMIC_YEAR'].isin(['2001 - 2002','2018 – 2019','2016 - 2017','2011 - 2012','2015 - 2016','2012 - 2013','2014 - 2015','2017 - 2018','2013 - 2014'])]

    ed = ed_final[['EMPLOYEE_NUMBER','SCHOOL']].drop_duplicates()

    entitlement = ed_final.groupby(['ACADEMIC_YEAR','EMPLOYEE_NUMBER']).agg({'TOTAL_ENTITLEMENT':'max','AMOUNT_TO_BE_PAID':'sum'})

    entitlement = entitlement.astype({'TOTAL_ENTITLEMENT':'float64'})

    entitlement['Diff'] = (entitlement['AMOUNT_TO_BE_PAID'] > entitlement['TOTAL_ENTITLEMENT']).astype('int')

    return entitlement[entitlement['Diff'] == 1]


# In[8]:


def schooling_equal():

    education = pd.read_sql(query_education, eng)

    education = education.astype({'AMOUNT_TO_BE_PAID':'float'})

    ed_final = education[['EMPLOYEE_NUMBER','ACADEMIC_YEAR','ACADEMIC_SEM','CHILD_NAME','DATE_OF_BIRTH','TOTAL_ENTITLEMENT','CHILD_ENTITLEMENT_AMOUNT','SCHOOL','AMOUNT_TO_BE_PAID']]

    ed = ed_final[['EMPLOYEE_NUMBER','SCHOOL']].drop_duplicates()

    entitlement = ed_final.groupby(['ACADEMIC_YEAR','EMPLOYEE_NUMBER']).agg({'TOTAL_ENTITLEMENT':'max','AMOUNT_TO_BE_PAID':'sum'})

    entitlement = entitlement.astype({'TOTAL_ENTITLEMENT':'float64'})

    entitlement['Diff'] = (entitlement['AMOUNT_TO_BE_PAID'] == entitlement['TOTAL_ENTITLEMENT']).astype('int')

    return entitlement[entitlement['Diff'] == 1]


# In[9]:


def check_details(EmpNo,Year):

    """"""

    education = pd.read_sql(query_education, eng)

    result = education[(education['EMPLOYEE_NUMBER'] == EmpNo) &  (education['ACADEMIC_YEAR'] == Year)]

    return result


# In[10]:


x = schooling_exceed()


# In[11]:


x.reset_index().to_sql('Schooling_Exceed',eng,if_exists='replace')


# In[ ]:

print('Success!')
