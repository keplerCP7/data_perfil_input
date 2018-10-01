
# coding: utf-8

# In[22]:


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

conf = SparkConf().setAppName("Perfiles-Training")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

codpais = sys.argv[1] #'CO' # sys.argv[1] ## #'PE' 
aniocampanaproceso = sys.argv[2] #'201710' #sys.argv[2] ## #'201801'


# In[23]:


df_data_set = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input/codpais=" + codpais + "/aniocampanaproceso=" + aniocampanaproceso + "/")
df_data_set.registerTempTable("df_data_set")

df_perfil_input = sqlContext.sql(" select '" + codpais + "' as codpais, '" + aniocampanaproceso + "' as aniocampanaproceso, * from df_data_set ")
df_perfil_input.registerTempTable("perfilinput")


# In[24]:


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


# In[25]:


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


# In[26]:


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
DataPerfil = pd.concat([data[(['codebelista'])],data_1,data_2,data4_P,data_5],axis=1)

DATA = DataPerfil[(['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'])]

DataPerfil1 = pd.concat([data_1,data_2,data4_P,data_5],axis=1)
DataPerfil1 = DataPerfil1.fillna(0)


# In[27]:


IQR = (DataPerfil1.quantile(.75)-DataPerfil1.quantile(0.25))
MAX1 =  DataPerfil1.quantile(.75) + 1.5*(IQR)
MAX1 = MAX1.transpose()
MIN1 = DataPerfil1.quantile(.25) -1.5*(IQR)
MIN1 = MIN1.transpose()
data_trat = DataPerfil1[(DataPerfil1 >= MIN1) &  (DataPerfil1 <= MAX1)]
data_trat = pd.concat([data[(['codebelista'])],data_trat],axis=1)


# In[28]:


## PREPARACION DE DATOS (POST MODELO).
data_CO = data_trat[data_trat.isnull().any(axis=1)]
data_CO.shape
##
data_SO = data_trat.dropna()
##
data_SO["outliers"] = 0
data_CO["outliers"] = 1

Dato_filt = data_CO.append(data_SO)
Dato_filt
Dato_filt= Dato_filt.sort_values(['codebelista'], ascending=[True])

Data_Perfil = pd.concat([DataPerfil,Dato_filt[(['outliers'])]],axis=1)

Data_entre = Data_Perfil[(Data_Perfil['outliers'] == 0)]
Data_reasig = Data_Perfil[(Data_Perfil['outliers'] == 1)]

DATA = Data_entre[(['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'])]
DATA_NEW = Data_reasig[(['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'])]

df_data = sqlContext.createDataFrame(DATA)


# In[30]:


#create output column features to pass ass parameter into th DF to kmeans
vecAssembler = VectorAssembler(inputCols=['pup_lbel','pup_cyzone','pup_cp','pup_mq','pup_tc','ppu_fg'], outputCol="features")
new_df = vecAssembler.transform(df_data)
#new_df.select("features").show()

#training kmeans model
kmeans = KMeans(k=6, seed=10)  
model = kmeans.fit(new_df.select('features'))

#transformed = model.transform(new_df)
#transformed.show()

#save model: uploading model (parquet format) to S3
model.write().overwrite().save("s3a://hdata-belcorp/perfiles-pickles/Modelo"+codpais+"/")


# In[3]:


#calculating pup marcas lbel, esika, cyzone
#df_pup_calculo = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
#                                " pup_lbel/(pup_lbel+pup_esika+pup_cyzone) as pup_lbel, "
#                                " pup_esika/(pup_lbel+pup_esika+pup_cyzone) as pup_esika, " 
#                                " pup_cyzone/(pup_lbel+pup_esika+pup_cyzone) as pup_cyzone "
#                                " from perfilinput ")
#df_pup_calculo.registerTempTable("pup_calculo")

#calculating pup categorias cp, mq, tf
#df_categoria_calculo =sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, " 
#                                " pup_cp/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_cp, "
#                                " pup_fg/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_fg, "
#                                " pup_mq/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_mq, "
#                                " pup_tc/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_tc, "
#                                " pup_tf/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_tf  "
#                                " from perfilinput ")
#df_categoria_calculo.registerTempTable("categoria_calculo")


#calculating MIN and MAX values for ppu indicators for brand and category lbel, exika, cyzone, 
#cp, fg, mq, tc, tf
#df_metricas_calculo = sqlContext.sql(" select max(ppu_lbel) as ppu_lbel_max, min(ppu_lbel) as ppu_lbel_min, " 
#                                     " max(ppu_esika) as ppu_esika_max, min(ppu_esika) as ppu_esika_min, " 
#                                     " max(ppu_cyzone) as ppu_cyzone_max, min(ppu_cyzone) as ppu_cyzone_min, " 
#                                    " max(ppu_cp) as ppu_cp_max, min(ppu_cp) as ppu_cp_min, "
#                                     " max(ppu_fg) as ppu_fg_max, min(ppu_fg) as ppu_fg_min, "
#                                     " max(ppu_mq) as ppu_mq_max, min(ppu_mq) as ppu_mq_min, "
#                                     " max(ppu_tc) as ppu_tc_max, min(ppu_tc) as ppu_tc_min, "
#                                     " max(ppu_tf) as ppu_tf_max, min(ppu_tf) as ppu_tf_min "
#                                     " from perfilinput ")
#df_metricas_calculo.registerTempTable("metricas_calculo")


# In[4]:


#calculating probability for ppup brand indicators lbel, esika, cyzone &
#ppu category cp, fg, mq, tc, tf
#df_homogenizacion_ppu_categoria_input = sqlContext.sql(" Select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
#                                                       
#                                         " (a.ppu_lbel - (select ppu_lbel_min from metricas_calculo))/((select ppu_lbel_max from metricas_calculo) - ( select ppu_lbel_min from metricas_calculo)) as ppu_lbel, "                                                       
#                                         " (a.ppu_esika - (select ppu_esika_min from metricas_calculo))/((select ppu_esika_max from metricas_calculo) - (select ppu_esika_min from metricas_calculo)) as ppu_esika, "
#                                         " (a.ppu_cyzone - (select ppu_cyzone_min from metricas_calculo))/((select ppu_cyzone_max from metricas_calculo) - (select ppu_cyzone_min from metricas_calculo)) as ppu_cyzone, "
#                                                       
#                                         " (a.ppu_cp - (select ppu_cp_min from metricas_calculo))/((select ppu_cp_max from metricas_calculo) - (select ppu_cp_min from metricas_calculo)) as ppu_cp, "
#                                         " (a.ppu_fg - (select ppu_fg_min from metricas_calculo))/((select ppu_fg_max from metricas_calculo) - (select ppu_fg_min from metricas_calculo)) as ppu_fg, "
#                                         " (a.ppu_mq - (select ppu_mq_min from metricas_calculo))/((select ppu_mq_max from metricas_calculo) - (select ppu_mq_min from metricas_calculo)) as ppu_mq, "
#                                         " (a.ppu_tc - (select ppu_tc_min from metricas_calculo))/((select ppu_tc_max from metricas_calculo) - (select ppu_tc_min from metricas_calculo)) as ppu_tc, "
#                                         " (a.ppu_tf - (select ppu_tf_min from metricas_calculo))/((select ppu_tf_max from metricas_calculo) - (select ppu_tf_min from metricas_calculo)) as ppu_tf "                                                       
#                                         
#                                         " from perfilinput a ")
#df_homogenizacion_ppu_categoria_input.registerTempTable("homogenizacion_ppu_categoria_input")


# In[18]:


#consolidating final data set for kmeans
#df_data_perfil = sqlContext.sql(" Select a.codpais, a.aniocampanaexposicion, a.aniocampanaproceso, a.codebelista, "
#                                 " b.pup_lbel, b.pup_esika, b.pup_cyzone , "
#                                 " c.pup_cp, c.pup_mq, c.pup_tc, c.pup_fg, c.pup_tf, "
#                                 " a.ppu_lbel, a.ppu_esika, a.ppu_cyzone , "
#                                 " a.ppu_cp, a.ppu_mq, a.ppu_tc, a.ppu_fg, a.ppu_tf "                 
#                                 " from homogenizacion_ppu_categoria_input a "
#                                 " inner join pup_calculo b on b.aniocampanaexposicion=a.aniocampanaexposicion and "
#                                 " b.aniocampanaproceso=a.aniocampanaproceso and b.codpais=a.codpais and "
#                                 " b.codebelista=a.codebelista "
#                                 " inner join categoria_calculo c on c.aniocampanaexposicion=a.aniocampanaexposicion and "
#                                 " c.aniocampanaproceso=a.aniocampanaproceso and c.codpais=a.codpais and "
#                                 " c.codebelista=a.codebelista")
#df_data_perfil.registerTempTable("data_perfil")
#DataPerfil1 = df_data_perfil.toPandas()


# In[6]:


#IQR = df_data_perfil.approxQuantile(["pup_lbel", "pup_cyzone", "pup_cp", "pup_mq", "pup_tc", "ppu_fg"], [.75], 0.25)
#df_IQR = sqlContext.sql(" select (percentile_approx(pup_lbel, 0.75) - percentile_approx(pup_lbel, 0.25))  as pup_lbel, "
#                     " (percentile_approx(pup_cyzone,0.75) - percentile_approx(pup_cyzone, 0.25)) as pup_cyzone, "
#                     " (percentile_approx(pup_esika,0.75) - percentile_approx(pup_esika, 0.25)) as pup_esika, "
#                     
#                     " (percentile_approx(pup_cp, 0.75) - percentile_approx(pup_cp, 0.25)) as pup_cp, "
#                     " (percentile_approx(pup_mq, 0.75) - percentile_approx(pup_mq, 0.25)) as pup_mq, "
#                     " (percentile_approx(pup_tc, 0.75) - percentile_approx(pup_tc, 0.25)) as pup_tc, "
#                     " (percentile_approx(pup_fg, 0.75) - percentile_approx(pup_fg, 0.25)) as pup_fg, "
#                     " (percentile_approx(pup_tf, 0.75) - percentile_approx(pup_tf, 0.25)) as pup_tf, "
#                     
#                     " (percentile_approx(ppu_lbel, 0.75) - percentile_approx(ppu_lbel, 0.25))  as ppu_lbel, "
#                     " (percentile_approx(ppu_cyzone, 0.75) - percentile_approx(ppu_cyzone, 0.25)) as ppu_cyzone, "
#                     " (percentile_approx(ppu_esika, 0.75) - percentile_approx(ppu_esika, 0.25)) as ppu_esika, "
#                     
#                     " (percentile_approx(ppu_cp, 0.75) - percentile_approx(ppu_cp, 0.25)) as ppu_cp, "
#                     " (percentile_approx(ppu_mq, 0.75) - percentile_approx(ppu_mq, 0.25)) as ppu_mq, "
#                     " (percentile_approx(ppu_tc, 0.75) - percentile_approx(ppu_tc, 0.25)) as ppu_tc, "                                         
#                     " (percentile_approx(ppu_fg, 0.75) - percentile_approx(ppu_fg, 0.25)) as ppu_fg, "
#                     " (percentile_approx(ppu_tf, 0.75) - percentile_approx(ppu_tf, 0.25)) as ppu_tf "
#                     
#                     " from data_perfil ")

#df_IQR.registerTempTable("df_IQR")


# In[7]:


#df_max1 = sqlContext.sql("Select "
#                         " (percentile_approx(pup_lbel, 0.75) + (1.5*(select pup_lbel from df_IQR))) as pup_lbel, "
#                         " (percentile_approx(pup_esika, 0.75) + (1.5*(select pup_esika from df_IQR))) as pup_esika, "
#                         " (percentile_approx(pup_cyzone, 0.75) + (1.5*(select pup_cyzone from df_IQR))) as pup_cyzone, "
#                         " (percentile_approx(pup_cp, 0.75) + (1.5*(select pup_cp from df_IQR))) as pup_cp, "
#                       " (percentile_approx(pup_mq, 0.75) + (1.5*(select pup_mq from df_IQR))) as pup_mq, "
#                         " (percentile_approx(pup_tc, 0.75) + (1.5*(select pup_tc from df_IQR))) as pup_tc, "
#                         " (percentile_approx(pup_tf, 0.75) + (1.5*(select pup_tf from df_IQR))) as pup_tf, "
#                         " (percentile_approx(ppu_lbel, 0.75) + (1.5*(select ppu_lbel from df_IQR))) as ppu_lbel, "
#                         " (percentile_approx(ppu_esika, 0.75) + (1.5*(select ppu_esika from df_IQR))) as ppu_esika, "
#                         " (percentile_approx(ppu_cyzone, 0.75) + (1.5*(select ppu_cyzone from df_IQR))) as ppu_cyzone, "                         
#                         " (percentile_approx(ppu_cp, 0.75) + (1.5*(select ppu_cp from df_IQR))) as ppu_cp, "
#                         " (percentile_approx(ppu_fg, 0.75) + (1.5*(select ppu_fg from df_IQR))) as ppu_fg, "
#                         " (percentile_approx(ppu_mq, 0.75) + (1.5*(select ppu_mq from df_IQR))) as ppu_mq, "
#                         " (percentile_approx(ppu_tc, 0.75) + (1.5*(select ppu_tc from df_IQR))) as ppu_tc, "
#                         " (percentile_approx(ppu_tf, 0.75) + (1.5*(select ppu_tf from df_IQR))) as ppu_tf "                         
#                         " from data_perfil ")
#df_max1.registerTempTable("df_max1")


# In[9]:


#df_min1 = sqlContext.sql("Select "
#                         " (percentile_approx(pup_lbel, 0.25) - (1.5*(select pup_lbel from df_IQR))) as pup_lbel, "
#                         " (percentile_approx(pup_esika, 0.25) - (1.5*(select pup_esika from df_IQR))) as pup_esika, "
#                         " (percentile_approx(pup_cyzone, 0.25) - (1.5*(select pup_cyzone from df_IQR))) as pup_cyzone, "
#                         " (percentile_approx(pup_cp, 0.25) - (1.5*(select pup_cp from df_IQR))) as pup_cp, "
#                         " (percentile_approx(pup_fg, 0.25) - (1.5*(select pup_fg from df_IQR))) as pup_fg, "
#                         " (percentile_approx(pup_mq, 0.25) - (1.5*(select pup_mq from df_IQR))) as pup_mq, "
#                         " (percentile_approx(pup_tc, 0.25) - (1.5*(select pup_tc from df_IQR))) as pup_tc, "
#                         " (percentile_approx(pup_tf, 0.25) - (1.5*(select pup_tf from df_IQR))) as pup_tf, "
#                         " (percentile_approx(ppu_lbel, 0.25) - (1.5*(select ppu_lbel from df_IQR))) as ppu_lbel, "
#                         " (percentile_approx(ppu_esika, 0.25) - (1.5*(select ppu_esika from df_IQR))) as ppu_esika, "
#                         " (percentile_approx(ppu_cyzone, 0.25) - (1.5*(select ppu_cyzone from df_IQR))) as ppu_cyzone, "                         
#                         " (percentile_approx(ppu_cp, 0.25) - (1.5*(select ppu_cp from df_IQR))) as ppu_cp, "
#                         " (percentile_approx(ppu_fg, 0.25) - (1.5*(select ppu_fg from df_IQR))) as ppu_fg, "
#                         " (percentile_approx(ppu_mq, 0.25) - (1.5*(select ppu_mq from df_IQR))) as ppu_mq, "
#                         " (percentile_approx(ppu_tc, 0.25) - (1.5*(select ppu_tc from df_IQR))) as ppu_tc, "
#                         " (percentile_approx(ppu_tf, 0.25) - (1.5*(select ppu_tf from df_IQR))) as ppu_tf "                         
#                         " from data_perfil ")
#df_min1.registerTempTable("df_min1")

