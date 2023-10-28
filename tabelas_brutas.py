# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
from discord_webhook import DiscordWebhook
import boto3


#chaves
AWS_KEY=os.environ["AWS_KEY"]
AWS_ACC=os.environ["AWS_ACC"]
AUTH_DISCORD_DATA=os.environ["AUTH_DISCORD_DATA"]

# %%
#Pegar JSON FILE da FC
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946634431?data=FCM")
    return response.json()

def personagens(id):
    response = req.get("https://xivapi.com/character/"+ id)
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

def discord():
    headers = {
        'authorization':AUTH_DISCORD_DATA
    }
    response = req.get(f"https://discord.com/api/v9/channels/1066456073504034966/messages", headers=headers)
    return response.json()

def discord_50_m(id):
    headers = {
        'authorization':AUTH_DISCORD_DATA
    }
    response = req.get(f"https://discord.com/api/v9/channels/1066456073504034966/messages?before=" + id + "&limit=50", headers=headers)
    return response.json()

# %%
FATO_MEMBROS_FC = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
FATO_MEMBROS_FC.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)

FATO_MEMBROS_FC["ID"] = FATO_MEMBROS_FC["ID"].astype(str) 
FATO_MEMBROS_FC["Lodestone"] = "https://na.finalfantasyxiv.com/lodestone/character/" + FATO_MEMBROS_FC["ID"] 

# %%
RAW_ClassJobs = pd.DataFrame([])
x=0
for x in range(0, FATO_MEMBROS_FC["ID"].count()):
    sup= pd.DataFrame(personagens(FATO_MEMBROS_FC["ID"][x])["Character"]["ClassJobs"])
    sup.drop(list(sup.filter(regex = "Exp")), axis = 1, inplace = True)
    sup.drop(["ClassID","IsSpecialised","JobID","UnlockedState"], axis =1, inplace = True)
    sup["Lodestone"] = "https://na.finalfantasyxiv.com/lodestone/character/" + FATO_MEMBROS_FC["ID"][x]
    RAW_ClassJobs = pd.concat([RAW_ClassJobs,sup])

RAW_ClassJobs.reset_index(drop = True, inplace = True)

# %%
RAW_discord=pd.DataFrame(discord())

x = RAW_discord["id"].min() 

while x != "1067307761966252043":
    sup = pd.DataFrame(discord_50_m(x))  
    x = sup["id"].min()
    RAW_discord = pd.concat([RAW_discord,sup])

RAW_discord.drop(["type","channel_id","author","attachments","embeds","mentions","mention_roles","pinned","mention_everyone","tts","edited_timestamp","flags","components","webhook_id"], axis =1 , inplace= True)    
RAW_discord["timestamp"] = RAW_discord["timestamp"].astype(str).str[:10]
sup = list(RAW_discord.columns)
sup[sup.index("content")] = "mensagem"
sup[sup.index("timestamp")] = "data"
RAW_discord.columns = sup
RAW_discord.reset_index(drop = True, inplace= True)

# %%
#Upload tabelas na AWS (bruto)
upload_s3("RAW_DISCORD_HISTORY.csv","client","dataff",RAW_discord)
upload_s3("RAW_ClassJobs.csv","client","dataff",RAW_ClassJobs)
upload_s3("FATO_MEMBROS_FC.csv","client","dataff",FATO_MEMBROS_FC)


