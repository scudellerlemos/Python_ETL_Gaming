# %%
import pandas as pd
import requests as req
import os
from io import StringIO, BytesIO
from discord_webhook import DiscordWebhook
import boto3
from bs4 import BeautifulSoup

# %%
# Chaves
AWS_KEY = os.getenv("AWS_KEY")
AWS_ACC = os.getenv("AWS_ACC")
WEBHOOK_SAIU_FC = os.getenv("WEBHOOK_SAIU_FC")
WEBHOOK_ENTROU_FC = os.getenv("WEBHOOK_ENTROU_FC")

# Função para pegar JSON FILE da FC
def dados_FC():
    # URL da página de membros da Free Company
    url = "https://na.finalfantasyxiv.com/lodestone/freecompany/9234349560946634431/member/"

    # Fazer a solicitação HTTP para obter o conteúdo da página
    response = req.get(url)
    response.raise_for_status()  # Verifica se a solicitação foi bem-sucedida

    # Analisar o conteúdo HTML da página
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar os membros na página
    members = []
    member_list = soup.find_all('div', class_='entry__freecompany__center')

    for member in member_list:
        name_tag = member.find('p', class_='entry__name')
        if name_tag:
            # Usando .string para acessar o texto dentro da tag
            name = name_tag.string.strip()
            members.append({'Name': name})
    return members

# Função para fazer upload no S3 AWS
def upload_s3(file, paste, bucket, df):
    s3_file_key = f"{paste}/{file}"
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACC, aws_secret_access_key=AWS_KEY)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=s3_file_key)

# Função para ler arquivo do S3 AWS
def read_csv_s3(file, paste, bucket):
    s3_file_key = f"{paste}/{file}"
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACC, aws_secret_access_key=AWS_KEY)
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    initial_df = pd.read_csv(BytesIO(obj['Body'].read()))
    return initial_df

# Função para enviar mensagens ao Discord
def send_discord_message(url, message):
        webhook = DiscordWebhook(url=url, content=message)
        response = webhook.execute()

# %%
# Obtenção dos dados atuais da FC
dados_atual = dados_FC()
dados_membros_df = pd.DataFrame(dados_atual)

# Criação da tabela de ontem
MEMBROS_FC_antes = read_csv_s3("RAW_MEMBROS_BACKUP.csv", "tabela_usuarios", "datafcgamingffxiv")

# Criação das listas das pessoas que entraram na FC ou saíram
lista_entrou = list(set(dados_membros_df['Name']) - set(MEMBROS_FC_antes['Name']))
lista_saiu = list(set(MEMBROS_FC_antes['Name']) - set(dados_membros_df['Name']))

# Criação dos dataframes de quem entrou e saiu
dados = pd.concat([dados_membros_df, MEMBROS_FC_antes], ignore_index=True).drop_duplicates(subset=["Name"])
dados_entrou = dados[dados['Name'].isin(lista_entrou)]
dados_saiu = dados[dados['Name'].isin(lista_saiu)]

# Reset index dos dataframes
dados_entrou.reset_index(drop=True, inplace=True)
dados_saiu.reset_index(drop=True, inplace=True)


# %%
# Upload do novo arquivo para o S3
upload_s3("RAW_MEMBROS_BACKUP.csv", "tabela_usuarios", "datafcgamingffxiv", dados_membros_df)

# %%
# Postagem das mensagens no Discord
for i, row in dados_entrou.iterrows():
    send_discord_message(WEBHOOK_ENTROU_FC, f"{row['Name']} (ID:{row['ID']}) entrou na fc.")

for i, row in dados_saiu.iterrows():
    send_discord_message(WEBHOOK_SAIU_FC, f"{row['Name']} (ID:{row['ID']}) saiu da fc.")


