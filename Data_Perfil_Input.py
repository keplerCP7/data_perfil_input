
# coding: utf-8

# In[1]:


import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Data-Perfil-Input")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

codpais = sys.argv[1] #'PE'# #'PE' 
aniocampanaproceso = sys.argv[2] #'201808'# #'201801'


# In[2]:


def CalculaAnioCampana(aniocampana, delta):
    resultado = str(int(aniocampana)).strip()
    numero = int(resultado[:4])*18 + int(resultado[-2:]) + delta
    anio = str(numero//18)
    campana =str(numero%18).zfill(2)
    resultado = anio + campana
    return resultado


# In[3]:


def calculo_campana_parquet_file(aniocampanaproceso):
    anio=int(aniocampanaproceso[:4])
    campana=int(aniocampanaproceso[-2:])
    if campana==1:
        campana_val=18
        anio=anio-1
        campana=str(anio) + str(campana_val)
    else:
        campana_val = campana - 1
        campana_val=str(campana_val)
        campana_val = '0'+ campana_val if len(campana_val) < 2 else campana_val
        campana = str(anio) + str(campana_val)
    
    return campana   


# In[4]:


aniocampanaproceso5 = calculo_campana_parquet_file(aniocampanaproceso)
aniocampanaproceso4 = calculo_campana_parquet_file(aniocampanaproceso5)
aniocampanaproceso3 = calculo_campana_parquet_file(aniocampanaproceso4)
aniocampanaproceso2 = calculo_campana_parquet_file(aniocampanaproceso3)
aniocampanaproceso1 = calculo_campana_parquet_file(aniocampanaproceso2)

#print(aniocampanaproceso1, aniocampanaproceso2, aniocampanaproceso3, aniocampanaproceso4, aniocampanaproceso5, aniocampanaproceso)


# In[6]:


#dmatrizcampana = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/")
#dwh_fvtaproebecam = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/")
dtipooferta = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dtipooferta/")
#dpais = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dpais/")
dproducto = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dproducto/")

#dmatrizcampana.registerTempTable("dmatrizcampana")
#dwh_fvtaproebecam.registerTempTable("dwh_fvtaproebecam")
dtipooferta.registerTempTable("dtipooferta")
#dpais.registerTempTable("dpais")
dproducto.registerTempTable("dproducto")


# In[7]:


#aniocampanaproceso=dwh_fvtaproebecam.agg({"aniocampana": "max"}).collect()[0][0]
aniocampanaproceso=str(aniocampanaproceso)
aniocampanaproceso_menos5 = CalculaAnioCampana(aniocampanaproceso, -5)
aniocampanaexposicion = aniocampanaproceso


# In[8]:


#reading fvtaproebecam files by partition last 6 campaigns
df_fvtaproebecam1 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso1 + "/")#\
df_fvtaproebecam2 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso2 + "/")#\
df_fvtaproebecam3 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso3 + "/")#\
df_fvtaproebecam4 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso4 + "/")#\
df_fvtaproebecam5 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso5 + "/")#\
df_fvtaproebecam6 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso + "/")#\
#.where('codpais == PE && (aniocampana >= 201701 && aniocampana <= 201706)')

df_fvtaproebecam1.registerTempTable("df_fvtaproebecam1")
df_fvtaproebecam2.registerTempTable("df_fvtaproebecam2")
df_fvtaproebecam3.registerTempTable("df_fvtaproebecam3")
df_fvtaproebecam4.registerTempTable("df_fvtaproebecam4")
df_fvtaproebecam5.registerTempTable("df_fvtaproebecam5")
df_fvtaproebecam6.registerTempTable("df_fvtaproebecam6")

df_fvtaproebecam = sqlContext.sql(" Select '" + codpais + "' as codpais, '" + aniocampanaproceso1 + "' as aniocampana, * from df_fvtaproebecam1 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso2 + "' as aniocampana, * from df_fvtaproebecam2 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso3 + "' as aniocampana, * from df_fvtaproebecam3 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso4 + "' as aniocampana, * from df_fvtaproebecam4 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso5 + "' as aniocampana, * from df_fvtaproebecam5 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso + "' as aniocampana, * from df_fvtaproebecam6 ")

df_fvtaproebecam.registerTempTable("fvtaproebecam")
#finish reading fvtaproebecam



#reading dmatrizcampana files by partition last 6 campaigns
df_dmatrizcampana1 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso1 + "/")#\
df_dmatrizcampana2 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso2 + "/")#\
df_dmatrizcampana3 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso3 + "/")#\
df_dmatrizcampana4 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso4 + "/")#\
df_dmatrizcampana5 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso5 + "/")#\
df_dmatrizcampana6 = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/codpais=" + codpais + "/aniocampana=" + aniocampanaproceso + "/")#\
#.where('codpais == PE && (aniocampana >= 201701 && aniocampana <= 201706)')

df_dmatrizcampana1.registerTempTable("df_dmatrizcampana1")
df_dmatrizcampana2.registerTempTable("df_dmatrizcampana2")
df_dmatrizcampana3.registerTempTable("df_dmatrizcampana3")
df_dmatrizcampana4.registerTempTable("df_dmatrizcampana4")
df_dmatrizcampana5.registerTempTable("df_dmatrizcampana5")
df_dmatrizcampana6.registerTempTable("df_dmatrizcampana6")

dmatrizcampana = sqlContext.sql(" Select '" + codpais + "' as codpais, '" + aniocampanaproceso1 + "' as aniocampana, * from df_dmatrizcampana1 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso2 + "' as aniocampana, * from df_dmatrizcampana2 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso3 + "' as aniocampana, * from df_dmatrizcampana3 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso4 + "' as aniocampana, * from df_dmatrizcampana4 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso5 + "' as aniocampana, * from df_dmatrizcampana5 union all "
                           " Select '" + codpais + "' as codpais, '" + aniocampanaproceso + "' as aniocampana, * from df_dmatrizcampana6 ")

dmatrizcampana.registerTempTable("dmatrizcampana")
#finish reading dmatrizcampana    

df_cusvalidos = sqlContext.sql(" Select a.CodPais, a.AnioCampana, a.CodVenta from dmatrizcampana a "
                               #" inner join dpais b on a.codpais=b.codpais "  
                               #" where a.codpais='" + codpais + "' and a.aniocampana >= '" + aniocampanaproceso_menos5 + "' "
                               #" and a.aniocampana <= '" + aniocampanaproceso + "' "
                               #" and a.codventa <> '00000' "
                               " where a.codventa <> '00000' "
                               " group by a.codpais, a.aniocampana, a.codventa ")

df_cusvalidos.registerTempTable("cusvalidos")

df_tipoofertavalidos = sqlContext.sql(" Select codpais, codtipooferta from dtipooferta "
                                   " where codtipooferta not in ('030','031','040','051','061', "
                                   " '062','065','066','068','071','072','077','078','079','082','083', "
                                   " '085','090','093','099','109','050','082','091','098') "
                                   " and codtipoprofit='01' "
                                   " and codpais = '" + codpais + "' "
                                   " group by codpais, codtipooferta ")

df_tipoofertavalidos.registerTempTable("tipoofertavalidos")


df_productos=sqlContext.sql(" Select codmarca, codsap from dproducto "
                              " where DesCategoria in ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE', " 
                              " 'TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL') and codmarca in ('A','B','C') ")

df_productos.registerTempTable("productos")

#df_fvtaproebecam = sqlContext.sql("select * from dwh_fvtaproebecam where codpais = '"+ codpais +"'")
#df_fvtaproebecam.registerTempTable("fvtaproebecam")

df_conteomarcas=sqlContext.sql(" Select '" + aniocampanaexposicion + "' aniocampanaexposicion,'" + aniocampanaproceso + "' as aniocampanaproceso "
                               " , a.codpais, a.codebelista, e.codmarca, count(distinct a.aniocampana) nropedidos "
                               " , sum(a.realuuvendidas) realuuvendidas, sum(a.realuuvendidas)/(count(distinct a.aniocampana)*1.0) pup "
                               " , sum(a.realvtamnneto/a.realtcpromedio)/(count(distinct a.aniocampana)*1.0) psp "
                               " , sum(a.realvtamnneto/a.realtcpromedio)/sum(a.realuuvendidas*1.0) ppu "
                               " from fvtaproebecam a inner join productos e on a.codsap=e.codsap "
                               " inner join cusvalidos cv on a.aniocampana = cv.aniocampana and a.codventa=cv.codventa "
                               " inner join tipoofertavalidos tof on a.codtipooferta = tof.codtipooferta "                        
                               " where a.aniocampana=a.aniocampanaref "
                               #" and a.codpais='" + codpais +"' and a.aniocampana between '" + aniocampanaproceso_menos5 +"' "
                               #" and '" + aniocampanaproceso +"' "
                               " group by a.codpais, a.codebelista, e.codmarca having sum(a.realuuvendidas)>0 ")

df_conteomarcas.registerTempTable("conteomarcas")

df_pivot1 = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarca, "
                           " max(cast(pup as decimal(12,4))) as pup, max(cast(ppu as decimal(12,4))) as ppu "
                           " from conteomarcas "
                           " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarca")

df_pivot1.registerTempTable("pivot")

df_tempomarcas_pup = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                 " sum(case when codmarca='A' then cast(pup as decimal(12,4)) else 0 end) as pup_lbel, "
                                 " sum(case when codmarca='B' then cast(pup as decimal(12,4)) else 0 end) as pup_esika, "
                                 " sum(case when codmarca='C' then cast(pup as decimal(12,4)) else 0 end) as pup_cyzone"
                                 " from pivot "
                                 " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcas_pup.registerTempTable("tempomarcas_pup")

df_tempomarcas_ppu = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                 " sum(case when codmarca='A' then cast(ppu as decimal(12,4)) else 0 end) as ppu_lbel, "
                                 " sum(case when codmarca='B' then cast(ppu as decimal(12,4)) else 0 end) as ppu_esika, "
                                 " sum(case when codmarca='C' then cast(ppu as decimal(12,4)) else 0 end) as ppu_cyzone"
                                 " from pivot "
                                 " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcas_ppu.registerTempTable("tempomarcas_ppu")

sqlContext.dropTempTable("pivot")

df_tempomarcas = sqlContext.sql(" Select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
                                " a.pup_lbel, a.pup_esika, a.pup_cyzone, b.ppu_lbel, b.ppu_esika, b.ppu_cyzone "
                                " from tempomarcas_pup a "
                                " inner join tempomarcas_ppu b on a.codpais=b.codpais and a.aniocampanaexposicion=b.aniocampanaexposicion "
                                " and a.aniocampanaproceso=b.aniocampanaproceso and a.codebelista=b.codebelista ")

df_tempomarcas.registerTempTable("tempomarcas")


sqlContext.dropTempTable("tempomarcas_ppu")
sqlContext.dropTempTable("tempomarcas_pup")

df_conteocategorias = sqlContext.sql(" select '" + aniocampanaexposicion + "' as aniocampanaexposicion, '" + aniocampanaproceso + "' as aniocampanaproceso, " 
               " a.codpais, a.codebelista, e.descategoria, case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
               " when e.descategoria = 'FRAGANCIAS' then 'FG' "
               " when e.descategoria = 'MAQUILLAJE' then 'MQ' "
               " when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
               " when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end codcategoria, "
               " count(distinct a.aniocampana) as nropedidos, "
               " sum(a.realuuvendidas) as realuuvendidas, "
               " sum(a.realuuvendidas)/(count(distinct a.AnioCampana)*1.0) as pup, "
               " sum(a.realvtamnneto/a.realtcpromedio)/(COUNT(distinct a.aniocampana)*1.0) as psp, "
               " sum(a.realVtamnneto/a.realtcpromedio)/SUM(a.realuuvendidas*1.0) as ppu "
               " from fvtaproebecam a inner join dproducto e on e.codsap=a.codsap "
               " inner join cusvalidos cv on a.codventa=cv.codventa "
               " inner join tipoofertavalidos tof on a.codpais=tof.codpais and a.codtipooferta=tof.codtipooferta "
               " where a.aniocampana=a.aniocampanaref " 
               #" and a.aniocampana between '" + aniocampanaproceso_menos5 + "' and '" + aniocampanaproceso + "' "
               " and e.descategoria in ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE','TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL') "
               " group by a.codpais, a.codebelista, e.descategoria, case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
                                                                          "when e.descategoria = 'FRAGANCIAS' then 'FG'"
                                                                          "when e.descategoria = 'MAQUILLAJE' then 'MQ' "
                                                                          "when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
                                                                          "when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end "
               " having sum(a.realuuvendidas)>0 ")

df_conteocategorias.registerTempTable("conteocategorias")

df_pivot1 = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codcategoria, "
                           " avg(cast(pup as decimal(12,4))) as pup, avg(cast(ppu as decimal(12,4))) as ppu "
                           " from conteocategorias "
                           " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codcategoria")

df_pivot1.registerTempTable("pivot")
sqlContext.dropTempTable("conteocategorias")    

df_tempocategoria_pup = sqlContext.sql("select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
               " sum(CASE WHEN codcategoria='CP' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cp, "
               " sum(CASE WHEN codcategoria='FG' THEN cast(pup as decimal(12,4)) else 0 end) as pup_fg, "
               " sum(CASE WHEN codcategoria='MQ' THEN cast(pup as decimal(12,4)) else 0 end) as pup_mq, "
               " sum(CASE WHEN codcategoria='TC' THEN cast(pup as decimal(12,4)) else 0 end) as pup_tc, "
               " sum(CASE WHEN codcategoria='TF' THEN cast(pup as decimal(12,4)) else 0 end) as pup_tf "
               " from pivot "
               "group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempocategoria_pup.registerTempTable("tempocategoria_pup")

df_tempocategoria_ppu = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
               "sum(CASE WHEN codcategoria='CP' THEN cast(ppu as decimal(12,4)) else 0 end) as ppu_cp, "
               "sum(CASE WHEN codcategoria='FG' THEN cast(ppu as decimal(12,4)) else 0 end) as ppu_fg, "
               "sum(CASE WHEN codcategoria='MQ' THEN cast(ppu as decimal(12,4)) else 0 end) as ppu_mq, "
               "sum(CASE WHEN codcategoria='TC' THEN cast(ppu as decimal(12,4)) else 0 end) as ppu_tc, "
               "sum(CASE WHEN codcategoria='TF' THEN cast(ppu as decimal(12,4)) else 0 end) as ppu_tf "
               "FROM pivot "
               "group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempocategoria_ppu.registerTempTable("tempoCategoria_ppu")
sqlContext.dropTempTable("pivot")

df_tempocategoria = sqlContext.sql(" select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
               " a.pup_cp, a.pup_fg, a.pup_mq, a.pup_tc, a.pup_tf, b.ppu_cp, b.ppu_fg, b.ppu_mq, b.ppu_tc, b.ppu_tf "
               " from tempocategoria_pup a "
               " inner join tempoCategoria_ppu b "
               " on a.codpais = b.codpais "
               " and a.aniocampanaexposicion = b.aniocampanaexposicion "
               " and a.aniocampanaproceso = b.aniocampanaproceso "
               " and a.codebelista= b.codebelista ")

df_tempocategoria.registerTempTable("tempocategoria")
sqlContext.dropTempTable("tempocategoria_pup")
sqlContext.dropTempTable("tempoCategoria_ppu")

df_conteomarcacategorias = sqlContext.sql(" select '" + aniocampanaexposicion + "' as aniocampanaexposicion, '" + aniocampanaproceso + "' as aniocampanaproceso, "
               " a.codpais, a.codebelista, e.codmarca, e.descategoria, case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
               " when e.descategoria = 'FRAGANCIAS' then 'FG' "
               " when e.descategoria = 'MAQUILLAJE' then 'MQ' "
               " when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
               " when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end codcategoria, "
               " rtrim(ltrim(e.codmarca))+case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
                                             "when e.descategoria = 'FRAGANCIAS' then 'FG' "
                                             "when e.descategoria = 'MAQUILLAJE' then 'MQ' "
                                             "when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
                                             "when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end codmarcacategoria, "
               " count(distinct a.aniocampana) as nropedidos, "
               " sum(a.realuuvendidas) as realuuvendidas, "
               " sum(a.realuuvendidas)/(count(distinct a.aniocampana)*1.0) as pup, "
               " sum(a.realvtamnneto/a.realtcpromedio)/(COUNT(distinct a.anioCampana)*1.0) as psp, "
               " sum(a.realvtamnneto/a.realtcpromedio)/SUM(a.realuuvendidas*1.0) as ppu "
               " from fvtaproebecam a "
               " inner join dproducto e on a.codsap=e.codsap "
               " inner join cusvalidos cv on a.codventa=cv.codventa "
               " inner join tipoofertavalidos tof on a.codpais = tof.codpais and a.codtipooferta = tof.codtipooferta "               
               " where a.aniocampana=a.aniocampanaref "
               #" and a.aniocampana between '" + aniocampanaproceso_menos5 +"' and '" + aniocampanaproceso + "' "
               " and e.codmarca in ('A','B','C') "
               " and e.descategoria in ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE','TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL') "
               " group by a.codpais, a.codebelista, e.codmarca, e.descategoria, case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
                                                                                      "when e.descategoria = 'FRAGANCIAS' then 'FG' "
                                                                                      "when e.descategoria = 'MAQUILLAJE' then 'MQ' "
                                                                                      "when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
                                                                                      "when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end "
               " having sum(a.realuuvendidas)>0 ")

df_conteomarcacategorias.registerTempTable("conteomarcacategorias")
sqlContext.dropTempTable("cusvalidos")
sqlContext.dropTempTable("tipoofertavalidos")    

df_pivot1 = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarcacategoria, "
               " avg(cast(pup as decimal(12,4))) as pup, avg(cast(ppu as decimal(12,4))) as ppu "
               " from conteomarcacategorias "
               " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarcacategoria ")

df_pivot1.registerTempTable("pivot")
sqlContext.dropTempTable("conteomarcacategorias")    

df_tempomarcacategoria_pup = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
               " sum(CASE WHEN codmarcacategoria='CCP' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cyzone_cp , "
               " sum(CASE WHEN codmarcacategoria='CFG' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cyzone_fg, "
               " sum(CASE WHEN codmarcacategoria='CMQ' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cyzone_mq, "
               " sum(CASE WHEN codmarcacategoria='CTC' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cyzone_tc, "
               " sum(CASE WHEN codmarcacategoria='CTF' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cyzone_tf, "
               " sum(CASE WHEN codmarcacategoria='BCP' THEN cast(pup as decimal(12,4)) else 0 end) as pup_esika_cp, "
               " sum(CASE WHEN codmarcacategoria='BFG' THEN cast(pup as decimal(12,4)) else 0 end) as pup_esika_fg, "
               " sum(CASE WHEN codmarcacategoria='BMQ' THEN cast(pup as decimal(12,4)) else 0 end) as pup_esika_mq, "
               " sum(CASE WHEN codmarcacategoria='BTC' THEN cast(pup as decimal(12,4)) else 0 end) as pup_esika_tc, "
               " sum(CASE WHEN codmarcacategoria='BTF' THEN cast(pup as decimal(12,4)) else 0 end) as pup_esika_tf, "
               " sum(CASE WHEN codmarcacategoria='ACP' THEN cast(pup as decimal(12,4)) else 0 end) as pup_lbel_cp, "
               " sum(CASE WHEN codmarcacategoria='AFG' THEN cast(pup as decimal(12,4)) else 0 end) as pup_lbel_fg, "
               " sum(CASE WHEN codmarcacategoria='AMQ' THEN cast(pup as decimal(12,4)) else 0 end) as pup_lbel_mq, "
               " sum(CASE WHEN codmarcacategoria='ATC' THEN cast(pup as decimal(12,4)) else 0 end) as pup_lbel_tc, "
               " sum(CASE WHEN codmarcacategoria='ATF' THEN cast(pup as decimal(12,4)) else 0 end) as pup_lbel_tf"
               " from pivot "
               " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcacategoria_pup.registerTempTable("tempomarcacategoria_pup")

df_tempomarcacategoria_ppu = sqlContext.sql(" SELECT aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
               " sum(CASE WHEN codmarcacategoria='CCP' THEN ppu else 0 end) ppu_cyzone_cp, "
               " sum(CASE WHEN codmarcacategoria='CFG' THEN ppu else 0 end) ppu_cyzone_fg, "
               " sum(CASE WHEN codmarcacategoria='CMQ' THEN ppu else 0 end) ppu_cyzone_mq, "
               " sum(CASE WHEN codmarcacategoria='CTC' THEN ppu else 0 end) ppu_cyzone_tc, "
               " sum(CASE WHEN codmarcacategoria='CTF' THEN ppu else 0 end) ppu_cyzone_tf, "
               " sum(CASE WHEN codmarcacategoria='BCP' THEN ppu else 0 end) ppu_esika_cp, "
               " sum(CASE WHEN codmarcacategoria='BFG' THEN ppu else 0 end) ppu_esika_fg, "
               " sum(CASE WHEN codmarcacategoria='BMQ' THEN ppu else 0 end) ppu_esika_mq, "
               " sum(CASE WHEN codmarcacategoria='BTC' THEN ppu else 0 end) ppu_esika_tc, "
               " sum(CASE WHEN codmarcacategoria='BTF' THEN ppu else 0 end) ppu_esika_tf, "
               " sum(CASE WHEN codmarcacategoria='ACP' THEN ppu else 0 end) ppu_lbel_cp, "
               " sum(CASE WHEN codmarcacategoria='AFG' THEN ppu else 0 end) ppu_lbel_fg, "
               " sum(CASE WHEN codmarcacategoria='AMQ' THEN ppu else 0 end) ppu_lbel_mq, "
               " sum(CASE WHEN codmarcacategoria='ATC' THEN ppu else 0 end) ppu_lbel_tc, "
               " sum(CASE WHEN codmarcacategoria='ATF' THEN ppu else 0 end) ppu_lbel_tf "
               " from pivot "
               " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcacategoria_ppu.registerTempTable("tempomarcacategoria_ppu")
sqlContext.dropTempTable("pivot")    

df_tempomarcacategoria = sqlContext.sql(" select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
               " a.pup_lbel_cp, a.pup_lbel_fg, a.pup_lbel_mq, a.pup_lbel_tc, a.pup_lbel_tf, a.pup_esika_cp, a.pup_esika_fg, "
               " a.pup_esika_mq, a.pup_esika_tc, a.pup_esika_tf, a.pup_cyzone_cp, a.pup_cyzone_fg, a.pup_cyzone_mq, "
               " a.pup_cyzone_tc, a.pup_cyzone_tf, b.ppu_lbel_cp, b.ppu_lbel_fg, b.ppu_lbel_mq, b.ppu_lbel_tc, b.ppu_lbel_tf, "
               " b.ppu_esika_cp, b.ppu_esika_fg, b.ppu_esika_mq, b.ppu_esika_tc, b.ppu_Esika_tf, b.ppu_cyzone_cp, b.ppu_cyzone_fg, "
               " b.ppu_cyzone_mq, b.ppu_cyzone_tc, b.ppu_cyzone_tf "
               " from tempomarcacategoria_pup a "
               " inner join tempomarcacategoria_ppu b "
               " on a.codpais = b.codpais "
               " and a.aniocampanaexposicion = b.aniocampanaexposicion "
               " and a.aniocampanaproceso = b.aniocampanaproceso "
               " and a.codebelista= b.codebelista ")

df_tempomarcacategoria.registerTempTable("tempomarcacategoria")
sqlContext.dropTempTable("tempomarcacategoria_pup")
sqlContext.dropTempTable("tempomarcacategoria_ppu")    

df_perfilinput = sqlContext.sql(" select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
               "a.pup_lbel, a.pup_esika, a.pup_cyzone, b.pup_cp, b.pup_fg, b.pup_mq, b.pup_tc, b.pup_tf, c.pup_lbel_cp, "
               "c.pup_lbel_fg, c.pup_lbel_mq, c.pup_lbel_tc, c.pup_lbel_tf, c.pup_esika_cp, c.pup_esika_fg, c.pup_esika_mq, "
               "c.pup_esika_tc, c.pup_esika_tf, c.pup_cyzone_cp, c.pup_cyzone_fg, c.pup_cyzone_mq, c.pup_cyzone_tc, c.pup_cyzone_tf, "
               "a.ppu_lbel, a.ppu_esika, a.ppu_cyzone, b.ppu_cp, b.ppu_fg, b.ppu_mq, b.ppu_tc, b.ppu_tf, c.ppu_lbel_cp, c.ppu_lbel_fg, "
               "c.ppu_lbel_mq, c.ppu_lbel_tc, c.ppu_lbel_tf, c.ppu_esika_cp, c.ppu_esika_fg, c.ppu_esika_mq, c.ppu_esika_tc, c.ppu_esika_tf, "
               "c.ppu_cyzone_cp, c.ppu_cyzone_fg, c.ppu_cyzone_mq, c.ppu_cyzone_tc, c.ppu_cyzone_tf, 0.00 as pup, 0.00 as ppu, 3 as tercilpup, 3 as tercilppu "
               "from tempomarcas a "
               "inner join tempocategoria b on a.codebelista = b.codebelista "
               "inner join tempomarcacategoria c on a.codebelista = c.codebelista ")

df_perfilinput.registerTempTable("perfilinput")    


# In[9]:


df_perfilinput.repartition("codpais", "aniocampanaexposicion").write              .partitionBy("codpais", "aniocampanaexposicion").mode("Append")              .parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")

