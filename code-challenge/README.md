# Code Challenge

O pipeline desenvolvido aqui foi usando ferramentas como Docker, Python e Spark. Não foi usado nenhuma ferramenta de orquestração de pipeline como Airflow ou Nifi devido ao tempo. 

O pipeline se resume a um script Spark (Python) que faz a extração das tabelas do banco que está no postgres no Docker-compose para uma pasta chamada data/tables. A extração pode ser feita diariamente ou total. Após a extração o script carrega o csv na pasta data chamado order_details.csv e faz a junção desses dados com os dados da tabela orders e grava numa tabela chamada order_details_complete no próprio banco postgres no docker-compose e salva também um csv chamado result_data.csv.

A primeira etapa pra se executar nesse pipe é a de configuração e instalação. Para isso, na pasta raiz basta digitar o seguinte comando:

```
make build

```

A segunda etapa é preparar o ambiente para ser executado. Basta digitar o seguinte comando também na pasta raiz:

```
make start_environment

```

A terceira etapa é escolher que tipo de pipe vai rodar, pipe completo ou por dia. Existe o por dia que pode ser executado da seguinte maneira:

```
make run_pipeline_date DATA=19980504

```

O completo basta digitar o seguinte comando:

```
make run_pipeline

```

O seguinte comando é para para todos os serviços:

```

make stop_environment

```


O Resutado será uma pasta chamada tables dentro da pasta data e outra chamada result_data.csv que tem o resultado final num arquivo csv.


** Obs: Um ponto importante é que para otmizar a leitura e escrita da tabela ORDERS foi utilizado a estratégia de particionar tanto na leitura quanto na escrita o que reduz bastante o volume de dados necessário pra rodar numa maquina. Nada impede que possa ser carregada por pedaços maiores caso haja um cluster com maior capacidade.
