# %%
import pandas as pd
import requests as req
import os
from io import StringIO
from io import BytesIO
from discord_webhook import DiscordWebhook
from github import Github

#chaves
WEBHOOK_SAIU_FC=os.environ["WEBHOOK_SAIU_FC"]
WEBHOOK_ENTROU_FC=os.environ["WEBHOOK_ENTROU_FC"]
GITHUB_PASSWORD=os.environ["G_PASSWORD"]
GITHUB_REPO=os.environ["G_REPO"]

# %%
#Pegar JSON FILE da FC
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946634431?data=FCM")
    return response.json()


# %%
#Criação da tabela de membros de hoje
MEMBROS_FC_DEPOIS = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
MEMBROS_FC_DEPOIS.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)


# %%
#Criação da tabela de ontem
url = "https://raw.githubusercontent.com/scudellerlemos/App_usuarios_last_santd/main/Scripts/RAW_MEMBROS_BACKUP.csv"
MEMBROS_FC_antes = pd.read_csv(url)


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
#Fazendo login no git
g = Github(GITHUB_PASSWORD)


# %%
#listando o repositorio
repo = g.get_user().get_repo(GITHUB_REPO)
all_files = []
contents = repo.get_contents("")
while contents:
    file_content = contents.pop(0)
    if file_content.type == "dir":
        contents.extend(repo.get_contents(file_content.path))
    else:
        file = file_content
        all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))

# %%
#gerando conteudo do arquivo do df em csv, mas sem gerar o arquivo. É guardado o conteudo na variavel str chamada "content"
csv_buf = StringIO()
MEMBROS_FC_DEPOIS.to_csv(csv_buf, header=True, index = False)
csv_buf.seek(0)
content=csv_buf.getvalue()

# %%
# Upload para github
git_prefix = 'Scripts/'
git_file = git_prefix + 'RAW_MEMBROS_BACKUP.csv'
if git_file in all_files:
    contents = repo.get_contents(git_file)
    repo.update_file(contents.path, "comitando arquivos", content, contents.sha, branch="main")
    print(git_file + ' Atualizado')
else:
    repo.create_file(git_file, "comitando arquivos", content, branch="main")
    print(git_file + ' Criado')


