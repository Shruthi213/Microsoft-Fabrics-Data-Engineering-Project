#!/usr/bin/env python
# coding: utf-8

# ## process-bing-news
# 
# New notebook

# In[24]:


df = spark.read.option("multiline", "true").json("Files/bing-latest-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/bing-latest-news.json".
display(df)


# In[25]:


df =df.select("value")


# In[26]:


display(df)


# In[27]:


from pyspark.sql.functions import explode

df_explode = df.select(explode(df["value"]).alias("json_object"))


# In[28]:


display(df_explode)


# In[29]:


json_list = df_explode.toJSON().collect()


# In[30]:


print(json_list[0])


# In[31]:


import json

news_json = json.loads(json_list[0])


# In[32]:


print(news_json)


# In[33]:


print(news_json['json_object']['name'])
print(news_json['json_object']['description'])
print(news_json['json_object']['datePublished'])
print(news_json['json_object']['url'])
print(news_json['json_object']['image']['thumbnail']['contentUrl'])
print(news_json['json_object']['provider'][0]['name'])


# In[34]:


title = []
description = []
datePublished = []
url = []
image = []
provider = []

# process each JSON object in the list
for json_str in json_list:
    try:
        #parse the string into the dictionary
        article = json.loads(json_str)

        if article["json_object"].get("image" , {}).get("thumbnail", {}).get("contentUrl"):

          # extract the information from the list
          title.append(article["json_object"]["name"])
          description.append(article["json_object"]["description"])
          datePublished.append(article["json_object"]["datePublished"])
          url.append(article["json_object"]["url"])
          image.append(article["json_object"]["image"])
          provider.append(article["json_object"]["provider"])

    except Exception as e:
        print(f"Error processing JSON object : {e}")


# In[35]:


title


# In[36]:


from pyspark.sql.types import StructType,StructField,StringType


#combine the lists
data = list(zip(title,description,datePublished,url))

#define schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("descrption", StringType(), True),
    StructField("datePublished", StringType(), True),
    StructField("url", StringType(), True),
    ##StructField("image", StringType(), True),
    ## StructField("provider", StringType(), True)

])

#create data frame
df_cleaned = spark.createDataFrame(data, schema=schema)


# In[37]:


display(df_cleaned)


# In[38]:


from pyspark.sql.functions import to_date, date_format

df_cleaned_final = df_cleaned.withColumn("datePublished", date_format(to_date("datePublished"), "dd-MM-yyyy"))


# In[39]:


display(df_cleaned_final)

