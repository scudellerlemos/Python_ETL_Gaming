# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
import boto3
import datetime as date
import numpy as np


#chaves
AWS_KEY=os.environ["AWS_KEY"]
AWS_ACC=os.environ["AWS_ACC"]

# %%
#Fazer upload na S3 AWS
def upload_s3(file,paste,bucket,df):
    s3_file_key = str(paste)+"/"+str(file)
    s3 = boto3.client("s3",aws_access_key_id=AWS_ACC, aws_secret_access_key=AWS_KEY)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index = False)
    csv_buf.seek(0)
    s3.put_object(Bucket=bucket,Body=csv_buf.getvalue(),Key=s3_file_key)

##Ler  arquivo na S3 AWS
def read_csv_s3(file,paste,bucket):
    s3_file_key = str(paste)+"/"+str(file)
    bucket = bucket
    s3 = boto3.client("s3",aws_access_key_id=AWS_ACC, aws_secret_access_key=AWS_KEY)
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    initial_df = pd.read_csv(BytesIO(obj['Body'].read()))
    return initial_df


# %%
#regras de classificação das tabelas
jobs_dict ={
                "alchemist / alchemist":"CRAFT",            
                "arcanist / scholar":"HLR",               
                "arcanist / summoner":"DPS",              
                "archer / bard":"DPS",   
                "armorer / armorer":"CRAFT",
                "astrologian / astrologian":"HLR",        
                "blacksmith / blacksmith":"CRAFT",          
                "blue mage / blue mage":"LIMITED",            
                "botanist / botanist":"GATHER",              
                "carpenter / carpenter":"CRAFT",            
                "conjurer / white mage":"HLR",            
                "culinarian / culinarian":"CRAFT",          
                "dancer / dancer":"DPS", 
                "dark knight / dark knight":"TNK",        
                "fisher / fisher":"GATHER", 
                "gladiator / paladin":"TNK",              
                "goldsmith / goldsmith":"CRAFT",            
                "gunbreaker / gunbreaker":"TNK",          
                "lancer / dragoon":"DPS",
                "leatherworker / leatherworker":"CRAFT",    
                "machinist / machinist":"DPS",            
                "marauder / warrior":"TNK",               
                "miner / miner":"GATHER",   
                "pugilist / monk":"DPS",
                "reaper / reaper":"DPS", 
                "red mage / red mage":"DPS",              
                "rogue / ninja":"DPS",   
                "sage / sage":"HLR",     
                "samurai / samurai":"DPS",
                "thaumaturge / black mage":"DPS",       
                "weaver / weaver":"HLR"      
            }

# %%
##ler tabela bruta RAW_ClassJobs
ANALYTICS_ClassJobs= read_csv_s3("RAW_ClassJobs.csv","client","dataff")


#Mudar nome da coluna "Name" para "Qtd_jobs"
sup = list(ANALYTICS_ClassJobs.columns)
sup[sup.index("Name")] = "Qtd_jobs"
ANALYTICS_ClassJobs.columns = sup

#Criando uma nova coluna de Jobs e aplicando replace
ANALYTICS_ClassJobs["Tipo_role"] = ANALYTICS_ClassJobs["Qtd_jobs"]
ANALYTICS_ClassJobs = ANALYTICS_ClassJobs.replace({"Tipo_role":jobs_dict})

#Agroupando tabelas e fazendo limpagem na coluna Lodestone

ANALYTICS_ClassJobs = ANALYTICS_ClassJobs.groupby(["Level","Lodestone","Tipo_role"])["Qtd_jobs"].count().reset_index()
ANALYTICS_ClassJobs["Lodestone"].replace(regex = "https://na.finalfantasyxiv.com/lodestone/character/", value = "", inplace = True)


#Mudando coluna Lodestone para ID (tipo int)
ANALYTICS_ClassJobs["Lodestone"] = ANALYTICS_ClassJobs["Lodestone"].astype(int)
sup = list(ANALYTICS_ClassJobs.columns)
sup[sup.index("Lodestone")] = "ID"
ANALYTICS_ClassJobs.columns = sup

#Criação da tabela de analytics
upload_s3("ANALYTICS_ClassJobs.csv","client","dataff",ANALYTICS_ClassJobs)

# %%
#Leitura do arquivo do historico do discord
ANALYTICS_DISCORD = read_csv_s3("RAW_DISCORD_HISTORY.csv","client","dataff")
ANALYTICS_DISCORD = ANALYTICS_DISCORD.sort_values(by="id")

#Tratamento de dados, vamos manter apenas o NOME, ID ,DATA e ,STATUS DE SAIDA E ENTRADA
ANALYTICS_DISCORD = ANALYTICS_DISCORD.join(ANALYTICS_DISCORD["mensagem"].str.split("(", expand = True))
sup = list(ANALYTICS_DISCORD.columns)
sup[sup.index(0)] = "mensagem_1"
sup[sup.index(1)] = "mensagem_2"
ANALYTICS_DISCORD.columns = sup
ANALYTICS_DISCORD.drop("mensagem", axis =1 , inplace = True)

ANALYTICS_DISCORD = ANALYTICS_DISCORD.join(ANALYTICS_DISCORD["mensagem_2"].str.split(")", expand = True))

sup = list(ANALYTICS_DISCORD.columns)
sup[sup.index(0)] = "mensagem_3"
sup[sup.index(1)] = "Status_entrada_saida"
ANALYTICS_DISCORD.columns = sup


ANALYTICS_DISCORD.drop(["mensagem_2","id"], axis =1 , inplace = True)
ANALYTICS_DISCORD["mensagem_3"].replace(regex = "ID:", value = "", inplace = True)

#Aplicando regex para simplificadar o status de entrada e saida da fc
ANALYTICS_DISCORD["Status_entrada_saida"].replace(regex = "entrou na fc.", value = "entrou", inplace = True)
ANALYTICS_DISCORD["Status_entrada_saida"].replace(regex = "saiu da fc.", value = "saiu", inplace = True)

#Renomeação de colunas para Name e ID
sup = list(ANALYTICS_DISCORD.columns)
sup[sup.index("mensagem_1")] = "Name"
sup[sup.index("mensagem_3")] = "ID"
ANALYTICS_DISCORD.columns = sup


# %%
#tratamendo de execessão, já existia uma tabela que não era atualizada regularmente e estava muito mal formatada
#foi uma decisão de trazer direto gravado do código ao inves de puxar o dataframe
for i in range(0,ANALYTICS_DISCORD["ID"].count()):
     if ANALYTICS_DISCORD["ID"][i] == "31418891":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "35516426":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "26767915":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "20959401":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "15475876":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "12831963":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "34960569":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "36626206":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"
     if ANALYTICS_DISCORD["ID"][i] == "36789814":
          ANALYTICS_DISCORD["data"][i] = "2022-11-30"

     if ANALYTICS_DISCORD["ID"][i] == "23359227":
          ANALYTICS_DISCORD["data"][i] = "2022-12-12"    
     if ANALYTICS_DISCORD["ID"][i] == "38653854":
          ANALYTICS_DISCORD["data"][i] = "2022-12-12"
     if ANALYTICS_DISCORD["ID"][i] == "45979075":
          ANALYTICS_DISCORD["data"][i] = "2022-12-12"
     if ANALYTICS_DISCORD["ID"][i] == "16733417":
          ANALYTICS_DISCORD["data"][i] = "2023-01-10"
     if ANALYTICS_DISCORD["ID"][i] == "30916698":
          ANALYTICS_DISCORD["data"][i] = "2022-12-06"
     if ANALYTICS_DISCORD["ID"][i] == "4994762":
          ANALYTICS_DISCORD["data"][i] = "2022-12-06"
     if ANALYTICS_DISCORD["ID"][i] == "14952764":
          ANALYTICS_DISCORD["data"][i] = "2023-01-18"
ANALYTICS_DISCORD=ANALYTICS_DISCORD.append({"data":"2023-01-11","Name":"Ca'leof Kael  ","ID":"14952764","Status_entrada_saida":"saiu"},ignore_index=True)
ANALYTICS_DISCORD=ANALYTICS_DISCORD.append({"data":"2023-01-17","Name":"Ca'leof Kael  ","ID":"14952764","Status_entrada_saida":"entrou"},ignore_index=True)
ANALYTICS_DISCORD = ANALYTICS_DISCORD.sort_values(by="data")
ANALYTICS_DISCORD = ANALYTICS_DISCORD.reset_index()

# %%
###Inicio da pivotação da coluna de Status_entrada e saida para Data_entrada e Data_saida
ANALYTICS_DISCORD['data']=pd.to_datetime(ANALYTICS_DISCORD["data"])


ANALYTICS_DISCORD["Entrou_saiu_2chave"] = float("NaN")
ANALYTICS_DISCORD["Entrou_saiu_3chave"] = float("NaN")
for x in list(ANALYTICS_DISCORD["Name"].unique()):
    sup = 1
    for y in list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x].reset_index()["index"]):
        if ANALYTICS_DISCORD["Status_entrada_saida"][y]== "entrou":
            sup+=1
        ANALYTICS_DISCORD["Entrou_saiu_2chave"][y]= sup

    for y in range(0, ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"].count()):
        try:
           if (ANALYTICS_DISCORD["Status_entrada_saida"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"])[y]] + ANALYTICS_DISCORD["Status_entrada_saida"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x].reset_index()["index"])[y-1]] + ANALYTICS_DISCORD["Status_entrada_saida"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"])[y+1]]) == "saiusaiuentrou":
               ANALYTICS_DISCORD["Entrou_saiu_3chave"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"])[y]] = "nok"
           else:
               ANALYTICS_DISCORD["Entrou_saiu_3chave"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"])[y]] = "ok"
        except:
           ANALYTICS_DISCORD["Entrou_saiu_3chave"][list(ANALYTICS_DISCORD.loc[ANALYTICS_DISCORD["Name"]== x ].reset_index()["index"])[y]] = "ok"



join_de_para = pd.DataFrame([])
join_de_para["id"] = ANALYTICS_DISCORD["ID"].unique()
join_de_para["Name"] = ANALYTICS_DISCORD["Name"].unique()



ANALYTICS_DISCORD = ANALYTICS_DISCORD.pivot(index=["Name","Entrou_saiu_2chave","Entrou_saiu_3chave"], columns="Status_entrada_saida",values="data").reset_index()


ANALYTICS_DISCORD = ANALYTICS_DISCORD.merge(join_de_para, how = "left", on= "Name")

ANALYTICS_DISCORD.drop(["Entrou_saiu_2chave","Entrou_saiu_3chave"],axis= 1, inplace=True)
sup = list(ANALYTICS_DISCORD.columns)
sup[1] = "Data_entrada"
sup[sup.index("saiu")] = "Data_saida"
ANALYTICS_DISCORD.columns = sup

for i in range(0,ANALYTICS_DISCORD["id"].count()):
    if pd.isnull(ANALYTICS_DISCORD["Data_entrada"][i])==True:
        ANALYTICS_DISCORD["Data_entrada"][i] = ANALYTICS_DISCORD["entrou"][i]

ANALYTICS_DISCORD.drop("entrou", axis =1 , inplace = True)

# %%
##Filtros em boll para condições abaixo
not_null_data_entrada = pd.isnull(ANALYTICS_DISCORD["Data_entrada"])==False
null_data_entrada = pd.isnull(ANALYTICS_DISCORD["Data_entrada"])==True
not_null_data_saida = pd.isnull(ANALYTICS_DISCORD["Data_saida"])==False
null_data_saida = pd.isnull(ANALYTICS_DISCORD["Data_saida"])==True

# %%
#calculos de dias se a pessoa está presente no grupo
ANALYTICS_DISCORD["Qtd_dias"]=float("NaN")
x=0
for x in range (x,ANALYTICS_DISCORD["id"].count()):
    if np.logical_and(not_null_data_entrada[x], null_data_saida[x])==True:
        sup = pd.Series([], dtype="object")
        sup[x] = (date.datetime.today() - ANALYTICS_DISCORD["Data_entrada"][x])
        ANALYTICS_DISCORD["Qtd_dias"][x] = sup.dt.components.days

# %%
#calculos de dias se a pessoa já saiu do grupo
x=0
ANALYTICS_DISCORD["Data_saida"] =  ANALYTICS_DISCORD["Data_saida"].astype(str)
ANALYTICS_DISCORD["Data_entrada"] =  ANALYTICS_DISCORD["Data_entrada"].astype(str)
for x in range (x,ANALYTICS_DISCORD["id"].count()):
    if np.logical_and(not_null_data_entrada[x], not_null_data_saida[x])==True:
        sup = pd.Series([], dtype="object")
        sup[x] = (date.datetime.strptime(ANALYTICS_DISCORD["Data_saida"][x],'%Y-%m-%d') - date.datetime.strptime(ANALYTICS_DISCORD["Data_entrada"][x],'%Y-%m-%d'))
        ANALYTICS_DISCORD["Qtd_dias"][x] = sup.dt.components.days

ANALYTICS_DISCORD["Data_entrada"] =  pd.to_datetime(ANALYTICS_DISCORD["Data_entrada"])
ANALYTICS_DISCORD["Data_saida"] =  pd.to_datetime(ANALYTICS_DISCORD["Data_saida"])

# %%
#upload to S3
upload_s3("ANALYTICS_DISCORD_HISTORY.csv","client","dataff",ANALYTICS_DISCORD)

# %%
#ler tabela fato
FATO_MEMBROS = read_csv_s3("FATO_MEMBROS_FC.csv","client","dataff")

# %%
#Leitura da tabela do discord
DISCORD = read_csv_s3("ANALYTICS_DISCORD_HISTORY.csv","client","dataff")
DISCORD = DISCORD[pd.isnull(DISCORD["Data_saida"])]

sup = list(DISCORD.columns)
sup[sup.index("id")]="ID"
DISCORD.columns = sup

# %%
#Filtros na tabela ClassJobs (LVL =90 e tem que ser Healer, Tank ou DPS)
CLASSJOBS = read_csv_s3("ANALYTICS_ClassJobs.csv","client","dataff")
CLASSJOBS = CLASSJOBS.query('Level ==90 & (Tipo_role == "HLR"  | Tipo_role == "TNK"  | Tipo_role == "DPS")')
CLASSJOBS = CLASSJOBS.groupby(["ID"]).agg({"Tipo_role":"count","Qtd_jobs":"sum"}).reset_index()

#join realizados para criação da tabela PROD_GERAL
ANALYTIC_GERAL = FATO_MEMBROS.merge(CLASSJOBS,how="left",on="ID")
ANALYTIC_GERAL = ANALYTIC_GERAL.merge(DISCORD,how="left",on="ID")

# %%
#Regras de cargos da FC
BATIDINHA = np.logical_and(np.logical_and(np.logical_and(ANALYTIC_GERAL["Qtd_dias"]>30,ANALYTIC_GERAL["Qtd_dias"]<60),ANALYTIC_GERAL["Tipo_role"]>=1),ANALYTIC_GERAL["Qtd_jobs"]>=1)
CERVEJA = np.logical_and(np.logical_and(ANALYTIC_GERAL["Qtd_dias"]>=60,ANALYTIC_GERAL["Tipo_role"]>=2),ANALYTIC_GERAL["Qtd_jobs"]>=2)
COPO = np.logical_or(pd.isnull(ANALYTIC_GERAL["Tipo_role"])==True,ANALYTIC_GERAL["Qtd_dias"]<=30)

# %%
ANALYTIC_GERAL["Rank_recomendado"] = float("NaN")

for i in range(0,ANALYTIC_GERAL["ID"].count()):
    if BATIDINHA[i] ==True:
        ANALYTIC_GERAL["Rank_recomendado"][i] = "Batidinha"
    if CERVEJA[i] ==True:
        ANALYTIC_GERAL["Rank_recomendado"][i] = "Cerveja"
    if COPO[i] ==True:
        ANALYTIC_GERAL["Rank_recomendado"][i] = "Copo"


ANALYTIC_GERAL.drop(["Name_y","Lodestone","Data_saida"],axis=1,inplace=True)

ANALYTIC_GERAL.columns
ANALYTIC_GERAL=ANALYTIC_GERAL[["ID","Avatar","Name_x","Rank","Rank_recomendado","Data_entrada","Qtd_dias","Tipo_role"]]

sup = list(ANALYTIC_GERAL)
sup[sup.index("Name_x")]="Name"
sup[sup.index("Tipo_role")]="Qtd_role_jobs"
ANALYTIC_GERAL.columns = sup

upload_s3("ANALYTIC_GERAL.csv","client","dataff",ANALYTIC_GERAL)


