# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
from discord_webhook import DiscordWebhook, DiscordEmbed
import boto3


#chaves
WEBHOOK_REGRAS_CARGOS=os.environ["WEBHOOK_REGRAS_CARGOS"]
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
PROD_CARGOS = read_csv_s3("PROD_CARGOS.csv","client","dataff")

# %%
webin = DiscordWebhook(url=WEBHOOK_REGRAS_CARGOS)
embed = DiscordEmbed(title='Alteração de Cargos', description='Estas são as pessoas que merecem nossa atenção para alteração de cargo no FF XIV.  (Alterar SOMENTE no jogo!!!)', color='ffa1b3')
embed.set_author(
    name="Gaj Shield",
    url="https://na.finalfantasyxiv.com/lodestone/character/31418891/",
    icon_url="https://img2.finalfantasyxiv.com/f/a331cfa93a83a2a0fcfc9fb0d9bf0e73_be20385e18333edb329d4574f364a1f0fc0_96x96.jpg?1674791220",
    )
embed.set_timestamp()
for i in range(0,PROD_CARGOS["Rank"].count()):
    embed.add_embed_field(name=str(PROD_CARGOS["Name"][i]), value="Alterar cargo "+ "**"+str(PROD_CARGOS["Rank"][i]) + "**"+ " para "+ "**"+str(PROD_CARGOS["Rank_recomendado"][i])+ "**" , inline=False)
webin.add_embed(embed)
response = webin.execute()



