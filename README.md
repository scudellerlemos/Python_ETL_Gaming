# Python_ETL_Gaming (Tema do jogo Final Fantasy XIV )

## Visão Geral

Este aplicativo é um ETL (Extract, Transform, Load) construído inteiramente em Python. 
Ele utiliza a infraestrutura da AWS para armazenar informações e enviar notificações para o Discord. Além disso, consome dados da API [xivapi.com](https://xivapi.com), que representa os dados do jogo Final Fantasy XIV.

## Funcionalidades Principais

- Extração, transformação e carga de dados.
- Consumo de dados da API [xivapi.com](https://xivapi.com).
- Divisão dos dados em três camadas: bruta, analítica e refinada.
- Arquitetura gratuita, utilizando a S3 bucket da AWS no free tier.

## Como Funciona

O aplicativo realiza a extração de dados da API [xivapi.com](https://xivapi.com), em seguida, processa e transforma esses dados em três camadas: bruta, analítica e refinada. 
Os dados brutos são armazenados na S3 bucket da AWS. Notificações sobre o processamento são enviadas para o Discord para manter os usuários informados.

## Produto final
 - Relatorio sobre quem saiu e entrou da free company (guilda)
 - Relatorio sobre os membros que precisam ter os cargos alterados da free company (favor adaptar o codigo a regra utilizada na sua free company)

## Como Usar

1. Clone este repositório.
2. Configure suas credenciais da AWS.
3. Personalize o código para atender às suas necessidades específicas.
4. Execute o aplicativo de acordo com o agendamento desejado.


Aproveite e utilize este aplicativo para automatizar e simplificar suas tarefas de ETL com facilidade e eficiência!
