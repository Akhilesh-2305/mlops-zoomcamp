#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import pandas as pd
import sklearn


# In[2]:


sklearn.__version__


# In[3]:


get_ipython().system('python -V')


# In[5]:


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


# In[6]:


get_ipython().system('pip show scikit-learn')


# In[7]:


get_ipython().system('pip install --upgrade scikit-learn==1.5.0')


# In[8]:


get_ipython().system('pip show scikit-learn')


# In[9]:


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


# In[10]:


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


# In[11]:


df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet')


# In[12]:


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


# In[13]:


y_pred


# In[14]:


y_pred.std()


# Q1. Notebook
# We'll start with the same notebook we ended up with in homework 1. We cleaned it a little bit and kept only the scoring part. You can find the initial notebook here.
# 
# Run this notebook for the March 2023 data.
# 
# What's the standard deviation of the predicted duration for this dataset?
# 
# 1.24
# 6.24
# 12.28
# 18.28

# ![Screenshot%202024-06-28%20115405.png](attachment:Screenshot%202024-06-28%20115405.png)

# Answer: 6.247

# In[16]:


year = 2023
month = 3

input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'


# In[17]:


get_ipython().system('mkdir output')


# In[18]:


df = read_data(input_file)
df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


# In[19]:


df.head()


# In[20]:


df_result = pd.DataFrame()
df_result['ride_id'] = df['ride_id']
df_result['predicted_duration'] = y_pred


# In[21]:


df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)


# In[22]:


get_ipython().system('ls -lh output')


# Q2. Preparing the output
# Like in the course videos, we want to prepare the dataframe with the output.
# 
# First, let's create an artificial ride_id column:
# 
# df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
# Next, write the ride id and the predictions to a dataframe with results.
# 
# Save it as parquet:
# 
# df_result.to_parquet(
#     output_file,
#     engine='pyarrow',
#     compression=None,
#     index=False
# )
# What's the size of the output file?
# 
# 36M
# 46M
# 56M
# 66M
# Note: Make sure you use the snippet above for saving the file. It should contain only these two columns. For this question, don't change the dtypes of the columns and use pyarrow, not fastparquet.

# ![Screenshot%202024-06-28%20120107.png](attachment:Screenshot%202024-06-28%20120107.png)

# Answer: 66M

# Q3. Creating the scoring script
# Now let's turn the notebook into a script.
# 
# Which command you need to execute for that?

# In[23]:


jupyter nbconvert --to script 04-homework.ipynb


# In[ ]:




