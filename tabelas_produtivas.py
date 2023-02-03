# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
import boto3

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
#Criação da Tabela de PROD CARGOS
ANALYTIC_GERAL= read_csv_s3("ANALYTIC_GERAL.csv","client","dataff")
membros_diff = ANALYTIC_GERAL["Rank"] != ANALYTIC_GERAL["Rank_recomendado"]
PROD_CARGOS = ANALYTIC_GERAL[membros_diff]

# %%
PROD_CARGOS["Rank"].replace("Blue Label",float("NaN"),inplace=True)
PROD_CARGOS["Rank"].replace("Platinum Label",float("NaN"),inplace=True)
PROD_CARGOS["Rank"].replace("Gold Label",float("NaN"),inplace=True)
PROD_CARGOS["Rank"].replace("Black Label",float("NaN"),inplace=True)
PROD_CARGOS["Rank"].replace("Red Label",float("NaN"),inplace=True)
PROD_CARGOS["Rank"].replace("Cerveja",float("NaN"),inplace=True)
PROD_CARGOS = PROD_CARGOS.dropna(subset=["Rank_recomendado","Rank"])

PROD_CARGOS.reset_index(drop=True,inplace=True)
upload_s3("PROD_CARGOS.csv","client","dataff",PROD_CARGOS)



