# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
from discord_webhook import DiscordWebhook
import boto3


#chaves
WEBHOOK_SAIU_FC=os.environ["WEBHOOK_SAIU_FC"]
WEBHOOK_ENTROU_FC=os.environ["WEBHOOK_ENTROU_FC"]
AWS_KEY=os.environ["AWS_KEY"]
AWS_ACC=os.environ["AWS_ACC"]

# %%
#Pegar JSON FILE da FC
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946634431?data=FCM")
    return response.json()

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
#Criação da tabela de membros de hoje
MEMBROS_FC_DEPOIS = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
MEMBROS_FC_DEPOIS.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)


# %%
#Criação da tabela de ontem
MEMBROS_FC_antes = read_csv_s3("RAW_MEMBROS_BACKUP.csv","client","dataff")


# %%
#Listas com os IDs das tabelas
Lista_membros_depois = list(MEMBROS_FC_DEPOIS["ID"])
Lista_membros_antes = list(MEMBROS_FC_antes["ID"])
Lista_membros_total = list(dict.fromkeys(Lista_membros_antes+Lista_membros_depois))


# %%
#Criação das listas das pessoas que entraram na FC ou sairam
lista_entrou = []
lista_saiu = []
lista_lixo = []
for ID in Lista_membros_total:
    ##saiu
    if ID in Lista_membros_antes:
        if ID in Lista_membros_depois:
            lista_lixo = []
        else:
            lista_saiu.append(ID)
    ##entrou
    else:
        if ID in Lista_membros_depois:
            lista_entrou.append(ID)
        else:
            lista_lixo = []

# %%
dados = pd.concat([MEMBROS_FC_DEPOIS,MEMBROS_FC_antes],ignore_index=True)
dados.drop_duplicates(subset=["ID"],inplace=True)
#Dataframe das pessoas que entraram da FC
dados_entrou=dados[dados['ID'].isin(lista_entrou)]
#Dataframe das pessoas que sairam da FC
dados_saiu=dados[dados['ID'].isin(lista_saiu)]

# %%
dados_entrou.reset_index(drop = True, inplace = True)
dados_saiu.reset_index(drop = True, inplace = True)


# %%
#postagem das mensagens no discord
if len(lista_entrou)>0:
    for i in range(0,len(lista_entrou)):
        webhook = DiscordWebhook(url=WEBHOOK_ENTROU_FC, content=str(dados_entrou["Name"][i]) +  "  (ID:"+ str(dados_entrou["ID"][i])+")  entrou na fc.")
        response = webhook.execute()  

# %%
##postagem das mensagens no discord
if len(lista_saiu)>0:
    for i in range(0,len(lista_saiu)):
        webhook = DiscordWebhook(url=WEBHOOK_SAIU_FC, content=str(dados_entrou["Name"][i]) +  "  (ID:"+ str(dados_entrou["ID"][i])+")  saiu da fc.")
        response = webhook.execute()   

# %%
upload_s3("RAW_MEMBROS_BACKUP.csv","client","dataff",MEMBROS_FC_DEPOIS)


