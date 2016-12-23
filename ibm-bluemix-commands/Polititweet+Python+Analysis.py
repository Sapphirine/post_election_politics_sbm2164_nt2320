# Twitter Sentiment App: Post-Election Results 
# The following commands were run on IBM BlueMix
# in the iPython notebook

# coding: utf-8

# In[3]:

#create an array that will hold the count for each sentiment
sentimentDistribution=[0] * 9
#For each sentiment, run a sql query that counts the number of tweets for which the sentiment score is greater than 60%
#Store the data in the array
for i, sentiment in enumerate(tweets.columns[-9:]):
    sentimentDistribution[i]=sqlContext.sql("SELECT count(*) as sentCount FROM tweets where " + sentiment + " > 60").collect()[0].sentCount


# In[5]:

# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *
 
# sc is an existing SparkContext.
sqlContext = SQLContext(sc)
 
parquetFile = sqlContext.read.parquet("swift://notebooks.spark/tweetsFull.parquet")
print parquetFile
 
parquetFile.registerTempTable("tweets");
sqlContext.cacheTable("tweets")
tweets = sqlContext.sql("SELECT * FROM tweets")
print tweets.count()
tweets.cache()


# In[7]:

#create an array that will hold the count for each sentiment
sentimentDistribution=[0] * 9
#For each sentiment, run a sql query that counts the number of tweets for which the sentiment score is greater than 60%
#Store the data in the array
for i, sentiment in enumerate(tweets.columns[-9:]):
    sentimentDistribution[i]=sqlContext.sql("SELECT count(*) as sentCount FROM tweets where " + sentiment + " > 60").collect()[0].sentCount


# In[8]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
 
ind=np.arange(9)
width = 0.35
bar = plt.bar(ind, sentimentDistribution, width, color='g', label = "distributions")
 
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
plt.ylabel('Tweet count')
plt.xlabel('Tone')
plt.title('Distribution of tweets by sentiments > 60%')
plt.xticks(ind+width, tweets.columns[-9:])
plt.legend()
 
plt.show()


# In[8]:

# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *
 
# sc is an existing SparkContext.
sqlContext = SQLContext(sc)
 
parquetFile = sqlContext.read.parquet("swift://notebooks.spark/tweetsFull.parquet")
print parquetFile
 
parquetFile.registerTempTable("tweets");
sqlContext.cacheTable("tweets")
tweets = sqlContext.sql("SELECT * FROM tweets WHERE text LIKE '%trump' OR text LIKE '%hillary'")
print tweets.count()
tweets.cache()

#create an array that will hold the count for each sentiment
sentimentDistribution=[0] * 9
#For each sentiment, run a sql query that counts the number of tweets for which the sentiment score is greater than 60%
#Store the data in the array
for i, sentiment in enumerate(tweets.columns[-9:]):
    sentimentDistribution[i]=sqlContext.sql("SELECT count(*) as sentCount FROM tweets where " + sentiment + " > 60").collect()[0].sentCount


# In[9]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
 
ind=np.arange(9)
width = 0.35
bar = plt.bar(ind, sentimentDistribution, width, color='g', label = "distributions")
 
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
plt.ylabel('Tweet count')
plt.xlabel('Tone')
plt.title('Distribution of tweets by sentiments > 60%')
plt.xticks(ind+width, tweets.columns[-9:])
plt.legend()
 
plt.show()


# In[ ]:



