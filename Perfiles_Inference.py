
# coding: utf-8

# In[1]:


import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel

#conf = SparkConf().setAppName("Perfiles-Training")
#sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

codpais = 'PE' #'CO' # sys.argv[1] ## # 
aniocampanaproceso = '201801' #'201710' #sys.argv[2] ## #


# In[2]:


df_data_set = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input/codpais=" + codpais + "/aniocampanaproceso=" + aniocampanaproceso + "/")
df_data_set.registerTempTable("df_data_set")

df_perfil_input = sqlContext.sql(" select '" + codpais + "' as codpais, '" + aniocampanaproceso + "' as aniocampanaproceso, * from df_data_set ")
df_perfil_input.registerTempTable("perfilinput")


# In[3]:


df_data = df_perfil_input.toPandas()
df_data[['pup_lbel']]=df_data[['pup_lbel']].astype(float)
df_data[['pup_esika']]=df_data[['pup_esika']].astype(float)
df_data[['pup_cyzone']]=df_data[['pup_cyzone']].astype(float)
df_data[['pup_cp']]=df_data[['pup_cp']].astype(float)
df_data[['pup_fg']]=df_data[['pup_fg']].astype(float)
df_data[['pup_mq']]=df_data[['pup_mq']].astype(float)
df_data[['pup_tc']]=df_data[['pup_tc']].astype(float)
df_data[['pup_tf']]=df_data[['pup_tf']].astype(float)
df_data[['pup_lbel_cp']]=df_data[['pup_lbel_cp']].astype(float)
df_data[['pup_lbel_fg']]=df_data[['pup_lbel_fg']].astype(float)
df_data[['pup_lbel_mq']]=df_data[['pup_lbel_mq']].astype(float)
df_data[['pup_lbel_tc']]=df_data[['pup_lbel_tc']].astype(float)
df_data[['pup_lbel_tf']]=df_data[['pup_lbel_tf']].astype(float)
df_data[['pup_esika_cp']]=df_data[['pup_esika_cp']].astype(float)
df_data[['pup_esika_fg']]=df_data[['pup_esika_fg']].astype(float)
df_data[['pup_esika_mq']]=df_data[['pup_esika_mq']].astype(float)
df_data[['pup_esika_tc']]=df_data[['pup_esika_tc']].astype(float)
df_data[['pup_esika_tf']]=df_data[['pup_esika_tf']].astype(float)
df_data[['pup_cyzone_cp']]=df_data[['pup_cyzone_cp']].astype(float)
df_data[['pup_cyzone_fg']]=df_data[['pup_cyzone_fg']].astype(float)
df_data[['pup_cyzone_mq']]=df_data[['pup_cyzone_mq']].astype(float)
df_data[['pup_cyzone_tc']]=df_data[['pup_cyzone_tc']].astype(float)
df_data[['pup_cyzone_tf']]=df_data[['pup_cyzone_tf']].astype(float)
df_data[['ppu_lbel']]=df_data[['ppu_lbel']].astype(float)
df_data[['ppu_esika']]=df_data[['ppu_esika']].astype(float)
df_data[['ppu_cyzone']]=df_data[['ppu_cyzone']].astype(float)
df_data[['ppu_cp']]=df_data[['ppu_cp']].astype(float)
df_data[['ppu_fg']]=df_data[['ppu_fg']].astype(float)
df_data[['ppu_mq']]=df_data[['ppu_mq']].astype(float)
df_data[['ppu_tc']]=df_data[['ppu_tc']].astype(float)
df_data[['ppu_tf']]=df_data[['ppu_tf']].astype(float)
df_data[['ppu_lbel_cp']]=df_data[['ppu_lbel_cp']].astype(float)
df_data[['ppu_lbel_fg']]=df_data[['ppu_lbel_fg']].astype(float)
df_data[['ppu_lbel_mq']]=df_data[['ppu_lbel_mq']].astype(float)
df_data[['ppu_lbel_tc']]=df_data[['ppu_lbel_tc']].astype(float)
df_data[['ppu_lbel_tf']]=df_data[['ppu_lbel_tf']].astype(float)
df_data[['ppu_esika_cp']]=df_data[['ppu_esika_cp']].astype(float)
df_data[['ppu_esika_fg']]=df_data[['ppu_esika_fg']].astype(float)
df_data[['ppu_esika_mq']]=df_data[['ppu_esika_mq']].astype(float)
df_data[['ppu_esika_tc']]=df_data[['ppu_esika_tc']].astype(float)
df_data[['ppu_esika_tf']]=df_data[['ppu_esika_tf']].astype(float)
df_data[['ppu_cyzone_cp']]=df_data[['ppu_cyzone_cp']].astype(float)
df_data[['ppu_cyzone_fg']]=df_data[['ppu_cyzone_fg']].astype(float)
df_data[['ppu_cyzone_mq']]=df_data[['ppu_cyzone_mq']].astype(float)
df_data[['ppu_cyzone_tc']]=df_data[['ppu_cyzone_tc']].astype(float)
df_data[['ppu_cyzone_tf']]=df_data[['ppu_cyzone_tf']].astype(float)
df_data[['pup']]=df_data[['pup']].astype(float)
df_data[['ppu']]=df_data[['ppu']].astype(float)
df_data[['tercilpup']]=df_data[['tercilpup']].astype(float)
df_data[['tercilppu']]=df_data[['tercilppu']].astype(float)


# In[4]:


data=df_data
data1 = (['pup_lbel', 'pup_esika', 'pup_cyzone'])
data1 = data[data1]
data2 = (['pup_cp', 'pup_fg', 'pup_mq', 'pup_tc', 'pup_tf'])
data2 = data[data2]
data3= (['pup_lbel_cp','pup_lbel_fg', 'pup_lbel_mq', 'pup_lbel_tc', 'pup_lbel_tf',
            'pup_esika_cp', 'pup_esika_fg', 'pup_esika_mq', 'pup_esika_tc',
            'pup_esika_tf', 'pup_cyzone_cp', 'pup_cyzone_fg', 'pup_cyzone_mq',
            'pup_cyzone_tc', 'pup_cyzone_tf'])
data3 = data[data3]
data4 = (['ppu_lbel', 'ppu_esika', 'ppu_cyzone'])
data4 = data[data4]
data5 = (['ppu_cp', 'ppu_fg', 'ppu_mq', 'ppu_tc', 'ppu_tf'])
data5 = data[data5]
data6 = (['ppu_lbel_cp','ppu_lbel_fg', 'ppu_lbel_mq', 'ppu_lbel_tc', 'ppu_lbel_tf','ppu_esika_cp', 
            'ppu_esika_fg', 'ppu_esika_mq', 'ppu_esika_tc','ppu_esika_tf', 'ppu_cyzone_cp', 'ppu_cyzone_fg', 
            'ppu_cyzone_mq','ppu_cyzone_tc', 'ppu_cyzone_tf'])
data6 = data[data6]


# In[5]:


sumdata1 = data1.sum(axis=1)
#sumdata1.head()
PUP_Lbel = data1["pup_lbel"]
PUP_Esika = data1["pup_esika"]
PUP_Cyzone = data1["pup_cyzone"]

#"{:.2f}".format(value)

PUP_Lbel_P = PUP_Lbel/sumdata1

PUP_Lbel_P = PUP_Lbel/sumdata1
PUP_Esika_P = PUP_Esika/sumdata1
PUP_Cyzone_P = PUP_Cyzone/sumdata1
data_1 = pd.DataFrame({'pup_lbel':PUP_Lbel_P, 'pup_esika':PUP_Esika_P, 'pup_cyzone':PUP_Cyzone_P})
###############################################################################################################################
sumdata2 = data2.sum(axis=1)
#sumdata2.head()

PUP_CP = data2["pup_cp"]
PUP_FG = data2["pup_fg"]
PUP_MQ = data2["pup_mq"]
PUP_TC = data2["pup_tc"]
PUP_TF = data2["pup_tf"]
PUP_CP_P = PUP_CP/sumdata2
PUP_FG_P = PUP_FG/sumdata2
PUP_MQ_P = PUP_MQ/sumdata2
PUP_TC_P = PUP_TC/sumdata2
PUP_TF_P = PUP_TF/sumdata2
data_2 = pd.DataFrame({'pup_cp':PUP_CP_P, 'pup_fg':PUP_FG_P, 'pup_mq':PUP_MQ_P,'pup_tc':PUP_TC_P,'pup_tf':PUP_TF_P})
##
data4_P  =  ((data4 - data4.min()) / (data4.max() - data4.min()))
###
data_5  =  ((data5 - data5.min()) / (data5.max() - data5.min()))
##
DataPerfil = pd.concat([data[(['codpais', 'aniocampanaexposicion', 'aniocampanaproceso', 'codebelista'])],data_1,data_2,data4_P,data_5],axis=1)

#DATA = DataPerfil[(['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'])]

DATA = DataPerfil[(['codpais', 'aniocampanaexposicion', 'aniocampanaproceso', 'codebelista', 'pup_lbel', 'pup_esika', 'pup_cyzone',
                              'pup_cp', 'pup_fg', 'pup_mq', 'pup_tc', 'pup_tf','ppu_lbel', 'ppu_esika',
                              'ppu_cyzone', 'ppu_cp', 'ppu_fg', 'ppu_mq', 'ppu_tc', 'ppu_tf'])]
df_data = sqlContext.createDataFrame(DATA)


# In[6]:


#create output column features to pass parameter into th DF to kmeans
vecAssembler = VectorAssembler(inputCols=['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'], outputCol="features")
new_df = vecAssembler.transform(df_data)#.select('codpais', 'aniocampanaexposicion', 'aniocampanaproceso', 'codebelista', 'features')

model = KMeansModel.load("s3://hdata-belcorp/perfiles-pickles/Modelo"+codpais)

transformed = model.transform(new_df)


#transformed.select("codpais", "codebelista", "pup_lbel", "pup_cyzone", "pup_cp", "ppu_fg", "prediction").show()


# In[7]:


df_perfil_final = transformed.selectExpr("codpais", "aniocampanaexposicion", "aniocampanaproceso", "codebelista" ,                                         "pup_lbel as pc_lb_pup", "pup_esika as pc_ek_pup",                                          "pup_cyzone as pc_cz_pup", "pup_cp as pc_cp_pup", "pup_fg as pc_fg_pup",                                          "pup_mq as pc_mq_pup", "pup_tc as pc_tc_pup", "pup_tf as pc_tf_pup",                                          "ppu_lbel as lb_ppu", "ppu_esika as ek_ppu", "ppu_cyzone as cz_ppu",                                          "ppu_cp as cp_ppu", "ppu_fg as fg_ppu", "ppu_mq as mq_ppu", "ppu_tc as tc_ppu",                                          "ppu_tf as tf_ppu", "prediction as perfil")


# In[8]:


df_perfil_final.repartition("codpais", "aniocampanaproceso").write              .partitionBy("codpais", "aniocampanaproceso").mode("Append")              .parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-output")

