### Banco de dados postgresql

Utiliza Docker Desktop. Para criar contâiner abra o prompt de comando com o Docker Desktop em execução e digite:
docker run --name postgresql -p 5437:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=ectt2025 -e POSTGRES_DB=ecttdb -d postgres:16.0


### Dados de acesso ao banco PostgreSQL

Host: localhost
Port: 5437
User: root
Password: ectt2025
database: ecttdb


### Link utilizado para exportação da planilha para CSV

https://docs.google.com/spreadsheets/d/1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ/export?format=csv&id=1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ&gid=0


### Link da exportação da planilha para XLSX, se desejado

https://docs.google.com/spreadsheets/d/1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ/export?format=xlsx&id=1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ&gid=0


### Principais atividades do projeto

1.	Conversão e formatação de campos do tipo data;
2.	Conversão e formatação de campos do tipo data e hora;
3.	Conversão e formatação de campos do tipo string removendo emojis e caracteres especiais;
4.	Conversão de dados em campos que representam tempo, de modo a exibir valores em horas e minutos utilizando como por exemplo, 01h;
5.	Criação de colunas adicionais com a representação de tempo decorrido em formato HH:MM que possibilita contabilização e cálculos com horas e minutos;
6.	Tratamento das colunas numéricas para converter números com virgula para números com pontos;
7.	Tratamento para converter valores booleanos;
8.	Remoção de todas as linhas em branco (todos os campos vazios) do dataframe.


### Arquivo do Power BI

Arquivo do Power BI está disponibilizado na pasta "pbix"

### Relatório de Atividades

O relatório de atividades encontra-se na pasta relatorio com o nome RTA#0001_AtividadeSoftex.pdf

