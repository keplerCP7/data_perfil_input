
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark import SQLContext

sqlContext = SQLContext(sparkContext=sc)


# In[35]:


def CalculaAnioCampana(aniocampana, delta):
    resultado = str(int(aniocampana)).strip()
    numero = int(resultado[:4])*18 + int(resultado[-2:]) + delta
    anio = str(numero//18)
    campana =str(numero%18).zfill(2)
    resultado = anio + campana
    return resultado


# In[3]:


codpais = 'PE' 
aniocampanaproceso = '201801'

aniocampanaproceso_menos5 = CalculaAnioCampana(aniocampanaproceso, -5)
aniocampanaexposicion = aniocampanaproceso


# In[4]:


dmatrizcampana = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dmatrizcampana/")
fvtaproebecam = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_fvtaproebecam/")
dtipooferta = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dtipooferta/")
dpais = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dpais/")
dproducto = sqlContext.read.parquet("s3://hdata-belcorp/modelo-analitico-parquet/dwh_dproducto/")


# In[5]:


dmatrizcampana.registerTempTable("dmatrizcampana")
fvtaproebecam.registerTempTable("fvtaproebecam")
dtipooferta.registerTempTable("dtipooferta")
dpais.registerTempTable("dpais")
dproducto.registerTempTable("dproducto")


# In[6]:


dpais_proceso = sqlContext.sql("Select codpais from dpais where codpais = '" + codpais + "'")


# In[7]:


df_cusvalidos = sqlContext.sql(" Select a.CodPais, a.AnioCampana, a.CodVenta from dmatrizcampana a "
                               " inner join dpais b on a.codpais=b.codpais "  
                               " where a.aniocampana >= '" + aniocampanaproceso_menos5 + "' "
                               " and a.aniocampana <= '" + aniocampanaproceso + "' "
                               " and a.codventa <> '00000' "
                               " group by a.codpais, a.aniocampana, a.codventa ")

df_cusvalidos.registerTempTable("cusvalidos")


# In[8]:


df_tipoofertavalidos = sqlContext.sql(" Select codpais, codtipooferta from dtipooferta "
                                   " where codtipooferta not in ('030','031','040','051','061', "
                                   " '062','065','066','068','071','072','077','078','079','082','083', "
                                   " '085','090','093','099','109','050','082','091','098') "
                                   " and codtipoprofit='01' "
                                   " and codpais = '" + codpais + "' "
                                   " group by codpais, codtipooferta ")

df_tipoofertavalidos.registerTempTable("tipoofertavalidos")


# In[9]:


df_productos=sqlContext.sql(" Select codmarca, codsap from dproducto "
                              " where DesCategoria in ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE', " 
                              " 'TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL') and codmarca in ('A','B','C') ")

df_productos.registerTempTable("productos")


# In[10]:


df_conteomarcas=sqlContext.sql(" Select '" + aniocampanaexposicion + "' aniocampanaexposicion,'" + aniocampanaproceso + "' as aniocampanaproceso "
                               " , a.codpais, a.codebelista, e.codmarca, count(distinct a.aniocampana) nropedidos "
                               " , sum(a.realuuvendidas) realuuvendidas, sum(a.realuuvendidas)/(count(distinct a.aniocampana)*1.0) pup "
                               " , sum(a.realvtamnneto/a.realtcpromedio)/(count(distinct a.aniocampana)*1.0) psp "
                               " , sum(a.realvtamnneto/a.realtcpromedio)/sum(a.realuuvendidas*1.0) ppu "
                               " from fvtaproebecam a inner join productos e on a.codsap=e.codsap "
                               " inner join cusvalidos cv on a.aniocampana = cv.aniocampana and a.codventa=cv.codventa "
                               " inner join tipoofertavalidos tof on a.codtipooferta = tof.codtipooferta "                        
                               " where a.aniocampana=a.aniocampanaref and a.codpais='" + codpais +"' and a.aniocampana between '" + aniocampanaproceso_menos5 +"' "
                               " and '" + aniocampanaproceso +"' "
                               " group by a.codpais, a.codebelista, e.codmarca having sum(a.realuuvendidas)>0 ")

df_conteomarcas.registerTempTable("conteomarcas")


# In[11]:


df_pivot1 = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarca, "
                           " max(cast(pup as decimal(12,4))) as pup, max(cast(ppu as decimal(12,4))) as ppu "
                           " from conteomarcas "
                           " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarca")

df_pivot1.registerTempTable("pivot")


# In[12]:


df_tempomarcas_pup = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                 " sum(case when codmarca='A' then cast(pup as decimal(12,4)) else 0 end) as pup_lbel, "
                                 " sum(case when codmarca='B' then cast(pup as decimal(12,4)) else 0 end) as pup_esika, "
                                 " sum(case when codmarca='C' then cast(pup as decimal(12,4)) else 0 end) as pup_cyzone"
                                 " from pivot "
                                 " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcas_pup.registerTempTable("tempomarcas_pup")


# In[13]:


df_tempomarcas_ppu = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                 " sum(case when codmarca='A' then cast(ppu as decimal(12,4)) else 0 end) as ppu_lbel, "
                                 " sum(case when codmarca='B' then cast(ppu as decimal(12,4)) else 0 end) as ppu_esika, "
                                 " sum(case when codmarca='C' then cast(ppu as decimal(12,4)) else 0 end) as ppu_cyzone"
                                 " from pivot "
                                 " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempomarcas_ppu.registerTempTable("tempomarcas_ppu")

sqlContext.dropTempTable("pivot")


# In[14]:


df_tempomarcas = sqlContext.sql(" Select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
                                " a.pup_lbel, a.pup_esika, a.pup_cyzone, b.ppu_lbel, b.ppu_esika, b.ppu_cyzone "
                                " from tempomarcas_pup a "
                                " inner join tempomarcas_ppu b on a.codpais=b.codpais and a.aniocampanaexposicion=b.aniocampanaexposicion "
                                " and a.aniocampanaproceso=b.aniocampanaproceso and a.codebelista=b.codebelista ")

df_tempomarcas.registerTempTable("tempomarcas")


sqlContext.dropTempTable("tempomarcas_ppu")
sqlContext.dropTempTable("tempomarcas_pup")


# In[15]:


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
               " where a.aniocampana=a.aniocampanaref and a.aniocampana between '" + aniocampanaproceso_menos5 + "' and '" + aniocampanaproceso + "' "
               " and e.descategoria in ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE','TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL') "
               " group by a.codpais, a.codebelista, e.descategoria, case when e.descategoria = 'CUIDADO PERSONAL' then 'CP' "
                                                                          "when e.descategoria = 'FRAGANCIAS' then 'FG'"
                                                                          "when e.descategoria = 'MAQUILLAJE' then 'MQ' "
                                                                          "when e.descategoria = 'TRATAMIENTO CORPORAL' then 'TC' "
                                                                          "when e.descategoria = 'TRATAMIENTO FACIAL' then 'TF' end "
               " having sum(a.realuuvendidas)>0 ")

df_conteocategorias.registerTempTable("conteocategorias")


# In[16]:


df_pivot1 = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codcategoria, "
                           " avg(cast(pup as decimal(12,4))) as pup, avg(cast(ppu as decimal(12,4))) as ppu "
                           " from conteocategorias "
                           " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codcategoria")

df_pivot1.registerTempTable("pivot")
sqlContext.dropTempTable("conteocategorias")


# In[17]:


df_tempocategoria_pup = sqlContext.sql("select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
               " sum(CASE WHEN codcategoria='CP' THEN cast(pup as decimal(12,4)) else 0 end) as pup_cp, "
               " sum(CASE WHEN codcategoria='FG' THEN cast(pup as decimal(12,4)) else 0 end) as pup_fg, "
               " sum(CASE WHEN codcategoria='MQ' THEN cast(pup as decimal(12,4)) else 0 end) as pup_mq, "
               " sum(CASE WHEN codcategoria='TC' THEN cast(pup as decimal(12,4)) else 0 end) as pup_tc, "
               " sum(CASE WHEN codcategoria='TF' THEN cast(pup as decimal(12,4)) else 0 end) as pup_tf "
               " from pivot "
               "group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")

df_tempocategoria_pup.registerTempTable("tempocategoria_pup")


# In[18]:


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


# In[19]:


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


# In[20]:


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
               " and a.aniocampana between '" + aniocampanaproceso_menos5 +"' and '" + aniocampanaproceso + "' "
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


# In[21]:


df_pivot1 = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarcacategoria, "
               " avg(cast(pup as decimal(12,4))) as pup, avg(cast(ppu as decimal(12,4))) as ppu "
               " from conteomarcacategorias "
               " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, codmarcacategoria ")

df_pivot1.registerTempTable("pivot")
sqlContext.dropTempTable("conteomarcacategorias")


# In[22]:


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


# In[23]:


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


# In[24]:


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


# In[25]:


#if codpais == 'TD':
#    cursor.execute("delete from dom_perfiles.mdl_perfilinput "
#                   "where AnioCampanaProceso = '" + anioCampanaProceso
#                   + "' AND AnioCampanaExposicion  = '" + anioCampanaExposicion + "';")
#else:
#    cursor.execute("delete from dom_perfiles.mdl_perfilinput "
#                   "where AnioCampanaProceso = '" + anioCampanaProceso
#                   + "' AND AnioCampanaExposicion  = '" + anioCampanaExposicion
#                   + "' AND CodPais = '" + codPais + "';")


# In[26]:


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


# In[27]:


#df_perfilinput.write.partitionBy("codpais").format("parquet").save("s3://hdata-belcorp/modelo-analitico-parquet/perfiles/perfil_input.parquet")


df = df_perfilinput.repartition(40, "codpais", "aniocampanaproceso")

#df_perfilinput.repartition(400).write.parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")

#df_perfilinput.write.csv("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")

#df_perfilinput.repartition("codpais", "aniocampanaproceso").write.partitionBy("codpais", "aniocampanaproceso").parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")


# In[34]:


#df.write.parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")
df_perfilinput.repartition("codpais", "aniocampanaexposicion").write.partitionBy("codpais", "aniocampanaexposicion").mode("Overwrite").parquet("s3://hdata-belcorp/modelo-analitico-parquet/perfiles-input")


# In[29]:


df_perfilinput.rdd.getNumPartitions()


# In[88]:



#spark.sql('describe perfilinput').show()
spark.sql("create hive table tblpq_perfilinput (  "
         "aniocampanaexposicion varchar(6), "
         "codebelista varchar(15), "
         "pup_lbel decimal(24,4), "
         "pup_esika decimal(24,4), "
         "pup_cyzone decimal(24,4), "
         "pup_cp decimal(24,4), "
         "pup_fg decimal(24,4), "
         "pup_mq decimal(24,4), "
         "pup_tc decimal(24,4), "
         "pup_tf decimal(24,4), "
         "pup_lbel_cp decimal(24,4), "
         "pup_lbel_fg decimal(24,4), "
         "pup_lbel_mq decimal(24,4), "
         "pup_lbel_tc decimal(24,4), "
         "pup_lbel_tf decimal(24,4), "
         "pup_esika_cp decimal(24,4), "
         "pup_esika_fg decimal(24,4), "
         "pup_esika_mq decimal(24,4), "
         "pup_esika_tc decimal(24,4), "
         "pup_esika_tf decimal(24,4), "
         "pup_cyzone_cp decimal(24,4), "
         "pup_cyzone_fg decimal(24,4), "
         "pup_cyzone_mq decimal(24,4), "
         "pup_cyzone_tc decimal(24,4), "
         "pup_cyzone_tf decimal(24,4), "
         "ppu_lbel decimal(24,4), "
         "ppu_esika decimal(24,4), "
         "ppu_cyzone decimal(24,4), "
         "ppu_cp decimal(24,4), "
         "ppu_fg decimal(24,4), "
         "ppu_mq decimal(24,4), "
         "ppu_tc decimal(24,4), "
         "ppu_tf decimal(24,4), "
         "ppu_lbel_cp decimal(28,8), "
         "ppu_lbel_fg decimal(28,8), "
         "ppu_lbel_mq decimal(28,8), "
         "ppu_lbel_tc decimal(28,8), "
         "ppu_lbel_tf decimal(28,8), "
         "ppu_esika_cp decimal(28,8), "
         "ppu_esika_fg decimal(28,8), "
         "ppu_esika_mq decimal(28,8), "
         "ppu_esika_tc decimal(28,8), "
         "ppu_esika_tf decimal(28,8), "
         "ppu_cyzone_cp decimal(28,8), "
         "ppu_cyzone_fg decimal(28,8), "
         "ppu_cyzone_mq decimal(28,8), "
         "ppu_cyzone_tc decimal(28,8), "
         "ppu_cyzone_tf decimal(28,8), "
         "pup decimal(2,2), "
         "ppu decimal(2,2), "
         "tercilpup integer, "
         "tercilppu integer) "
         "PARTITIONED BY(codpais varchar(2), aniocampanaproceso varchar(6)) "
         "STORED AS PARQUET "
         "LOCATION 's3://hdata-belcorp/parquet-campaign-manager/perfil-input/' "
         "tblproperties ('parquet.compress'='SNAPPY')")


# In[28]:


df_pup_calculo = sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                " pup_lbel/(pup_lbel+pup_esika+pup_cyzone) as pup_lbel, "
                                " pup_esika/(pup_lbel+pup_esika+pup_cyzone) as pup_esika, " 
                                " pup_cyzone/(pup_lbel+pup_esika+pup_cyzone) as pup_cyzone "
                                " from perfilinput ")

df_pup_calculo.registerTempTable("pup_calculo")


# In[29]:


df_categoria_calculo =sqlContext.sql(" Select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, " 
                                " pup_cp/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_cp, pup_fg/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_fg, "
                                " pup_mq/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_mq, pup_tc/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_tc, "
                                " pup_tf/(pup_cp+pup_fg+pup_mq+pup_tc+pup_tf) as pup_tf  "
                                " from perfilinput ")

df_categoria_calculo.registerTempTable("categoria_calculo")


# In[30]:


df_metricas_calculo = sqlContext.sql(" select aniocampanaexposicion, aniocampanaproceso, codpais, codebelista, "
                                     " max(ppu_lbel) as ppu_lbel_max, min(ppu_lbel) as ppu_lbel_min, " 
                                     " max(ppu_esika) as ppu_esika_max, min(ppu_esika) as ppu_esika_min, " 
                                     " max(ppu_cyzone) as ppu_cyzone_max, min(ppu_cyzone) as ppu_cyzone_min, "
                                     " max(ppu_cp) as ppu_cp_max, min(ppu_cp) as ppu_cp_min, "
                                     " max(ppu_fg) as ppu_fg_max, min(ppu_fg) as ppu_fg_min, "
                                     " max(ppu_mq) as ppu_mq_max, min(ppu_mq) as ppu_mq_min, "
                                     " max(ppu_tc) as ppu_tc_max, min(ppu_tc) as ppu_tc_min, "
                                     " max(ppu_tf) as ppu_tf_max, min(ppu_tf) as ppu_tf_min "
                                     " from perfilinput "
                                     " group by aniocampanaexposicion, aniocampanaproceso, codpais, codebelista ")
df_metricas_calculo.registerTempTable("metricas_calculo")


# In[31]:


df_homogenizacion_ppu_marca_input = sqlContext.sql(" Select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
                                         " (a.ppu_lbel - b.ppu_lbel_min)/(b.ppu_lbel_max - b.ppu_lbel_min) as ppu_lbel, "
                                         " (a.ppu_esika - b.ppu_esika_min)/(b.ppu_esika_max - b.ppu_esika_min) as ppu_esika, "
                                         " (a.ppu_cyzone - b.ppu_cyzone_min)/(b.ppu_cyzone_max - b.ppu_cyzone_min) as ppu_cyzone "
                                         " from perfilinput a "
                                         " inner join metricas_calculo b on b.aniocampanaexposicion=a.aniocampanaexposicion and "
                                         " b.aniocampanaproceso=a.aniocampanaproceso and b.codpais=a.codpais and "
                                         " b.codebelista=a.codebelista ")

df_homogenizacion_ppu_marca_input.registerTempTable("homogenizacion_ppu_marca_input")


# In[32]:


df_homogenizacion_ppu_categoria_input = sqlContext.sql(" Select a.aniocampanaexposicion, a.aniocampanaproceso, a.codpais, a.codebelista, "
                                                       
                                         " (a.ppu_lbel - b.ppu_lbel_min)/(b.ppu_lbel_max - b.ppu_lbel_min) as ppu_lbel, "                                                       
                                         " (a.ppu_esika - b.ppu_esika_min)/(b.ppu_esika_max - b.ppu_esika_min) as ppu_esika, "
                                         " (a.ppu_cyzone - b.ppu_cyzone_min)/(b.ppu_cyzone_max - b.ppu_cyzone_min) as ppu_cyzone, "
                                                       
                                         " (a.ppu_cp - b.ppu_cp_min)/(b.ppu_cp_max - b.ppu_cp_min) as ppu_cp, "
                                         " (a.ppu_fg - b.ppu_fg_min)/(b.ppu_fg_max - b.ppu_fg_min) as ppu_fg, "
                                         " (a.ppu_mq - b.ppu_mq_min)/(b.ppu_mq_max - b.ppu_mq_min) as ppu_mq, "
                                         " (a.ppu_tc - b.ppu_tc_min)/(b.ppu_tc_max - b.ppu_tc_min) as ppu_tc, "
                                         " (a.ppu_tf - b.ppu_tf_min)/(b.ppu_tf_max - b.ppu_tf_min) as ppu_tf "                                                       
                                         
                                         " from perfilinput a "
                                         " inner join metricas_calculo b on b.aniocampanaexposicion=a.aniocampanaexposicion and "
                                         " b.aniocampanaproceso=a.aniocampanaproceso and b.codpais=a.codpais and "
                                         " b.codebelista=a.codebelista ")

df_homogenizacion_ppu_categoria_input.registerTempTable("homogenizacion_ppu_categoria_input")


# In[54]:


df_data_perfil = sqlContext.sql(" Select b.pup_lbel, b.pup_cyzone , c.pup_cp, c.pup_mq, c.pup_tc, a.ppu_fg "                                                       
                                 " from homogenizacion_ppu_categoria_input a "
                                 " inner join pup_calculo b on b.aniocampanaexposicion=a.aniocampanaexposicion and "
                                 " b.aniocampanaproceso=a.aniocampanaproceso and b.codpais=a.codpais and "
                                 " b.codebelista=a.codebelista "
                                 " inner join categoria_calculo c on c.aniocampanaexposicion=a.aniocampanaexposicion and "
                                 " c.aniocampanaproceso=a.aniocampanaproceso and c.codpais=a.codpais and "
                                 " c.codebelista=a.codebelista")

df_data_perfil.registerTempTable("data_perfil")


# In[57]:


pdf = df_data_perfil.toPandas()


# In[37]:


import boto3
import boto3.session

session = boto3.session.Session(region_name='us-east-1')
s3client = session.client('s3', config= boto3.session.Config(signature_version='s3v4'))

response = s3client.get_object(Bucket='hdata-belcorp', Key='perfiles-pickles/'+ "Modelo" + codpais + ".pkl"'')


# In[38]:


import pickle

body_string = response['Body'].read()
kmeans = pickle.loads(body_string)


# In[43]:


pdf = df_homogenizacion_ppu_categoria_input.toPandas()


# In[45]:


pdf.ppu_lbel.astype='float'


# In[58]:



predict  = kmeans.predict(pdf)


# In[33]:


df_homogenizacion_ppu_categoria_input.printSchema()


# In[34]:


import boto3
import boto3.session


# In[106]:


df_perfilinput.printSchema()


# In[49]:


sqlContext.dropTempTable("conteocategorias")


# In[26]:


#ventas_consultora = sqlContext.sql("SELECT * FROM parquetFile where codpais='CO' and aniocampana='201807' limit 100")
ventas_consultora = sqlContext.sql("SELECT codpais, aniocampana, codebelista FROM fvtaproebecam where codpais='PE' and aniocampana='201707' limit 100")
cols1 = ['codpais', 'codebelista']
ventas_consultora.select(cols1).show()


# In[27]:


dproducto.


# In[11]:


ventas_all = ventas_consultora.map(lambda p: "codebelista:".format(p.codebelista, p.codpais)) #.format(p.name, p.comment_col))


# In[12]:


for nation in ventas_consultora.collect():
    print(nation)

