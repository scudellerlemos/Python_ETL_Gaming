# %%
import pandas as pd
import requests as req
import os


# %%
#Pegar JSON FILE da FC
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946634431?data=FCM")
    return response.json()


#CHAVE DE ENTRADA DO USUARIO POSTAR NO DISCORD
headers = {
            'authorization':'MTgxMTQxOTc4NDI2MjQ1MTQx.GOknMc.Crpes8TnnWGWZUwldkd7BMxwwvz1aAAlXG6CUY'}

# %%
#Criação da tabela de membros de hoje
MEMBROS_FC_DEPOIS = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
MEMBROS_FC_DEPOIS.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)


# %%
#Criação da tabela de ontem
url = "https://raw.githubusercontent.com/scudellerlemos/App_usuarios_last_santd/main/Scripts/RAW_MEMBROS_ONTEM.csv"
MEMBROS_FC_antes = pd.read_csv(url)
MEMBROS_FC_antes.drop("Unnamed: 0",axis=1,inplace=True)


# %%
#Listas com os IDs das tabelas
Lista_membros_depois = list(MEMBROS_FC_DEPOIS["ID"])
Lista_membros_antes = list(MEMBROS_FC_antes["ID"])
Lista_membros_total = list(dict.fromkeys(Lista_membros_antes+Lista_membros_depois))


# %%
#Criação das listas das pessoas que entraram na FC ou sairam
lista_entrou=[]
lista_saiu=[]
lista_lixo=[]
for ID in Lista_membros_total:
    ##saiu
    if ID in Lista_membros_antes:
        if ID in Lista_membros_depois:
            lista_lixo=[]
        else:
            lista_saiu.append(ID)
    ##entrou
    else:
        if ID in Lista_membros_depois:
            lista_entrou.append(ID)
        else:
            lista_lixo=[]



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
        payload = {
        'content': str(dados_entrou["Name"][i]) +  "  (ID:"+ str(dados_entrou["ID"][i])+")  entrou na fc."
        }
        response = req.post(f"https://discord.com/api/v9/channels/1066456073504034966/messages",data=payload, headers=headers)
        response.json()    

# %%
##postagem das mensagens no discord
if len(lista_saiu)>0:
    for i in range(0,len(lista_saiu)):
        payload = {
        'content':str(dados_saiu["Name"][i]) +  "  (ID:"+ str(dados_saiu["ID"][i])+")  saiu da fc."
        }
        response = req.post(f"https://discord.com/api/v9/channels/1066455910228164690/messages",data=payload, headers=headers)
        response.json()   

# %%
from github import Github

g = Github(repo-token)


# %%

repo = g.get_user().get_repo("App_usuarios_last_santd")
all_files = []
contents = repo.get_contents("")
while contents:
    file_content = contents.pop(0)
    if file_content.type == "dir":
        contents.extend(repo.get_contents(file_content.path))
    else:
        file = file_content
        all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))

file = open("RAW_MEMBROS_ONTEM.CSV")
content = file.read()

# Upload to github
git_prefix = 'Scripts/'
git_file = git_prefix + 'RAW_MEMBROS_ONTEM.csv'
if git_file in all_files:
    contents = repo.get_contents(git_file)
    repo.update_file(contents.path, "committing files", content, contents.sha, branch="main")
    print(git_file + ' UPDATED')
else:
    repo.create_file(git_file, "committing files", content, branch="main")
    print(git_file + ' CREATED')


