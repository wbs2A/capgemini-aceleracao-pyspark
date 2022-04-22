# Capgemini - Aceleração PySpark 2022

Este projeto é parte do Programa de Aceleração [PySpark](https://spark.apache.org) da [Capgemini Brasil](https://www.capgemini.com/br-pt).
[<img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" align="right" width="140">](https://www.capgemini.com/br-pt)

## Sobre

Este projeto consiste em realizar tarefas que buscam garantir a qualidade dos dados para responder perguntas de negócio a fim de gerar relatórios de forma assertiva. As tarefas são essencialmente apontar inconsistências nos dados originais, e realizar transformações que permitam tratar as inconsistências e enriquecer os dados. Em resumo, o projeto está organizado em três módulos: (1) qualidade, (2) transformação, e (3) relatório.

## Dependências

Para executar os Jupyter Notebooks deste repositório é necessário ter o [Spark instalado localmente](https://spark.apache.org/downloads.html) e também as seguintes dependências:

`pip install pyspark findspark`

# Desafio final

Neste repositório também contém os arquivos do projeto referente ao desafio final que, de maneira interdisciplinar, visa colocar em prática o conhecimento das últimas semanas ao realizar tarefas que buscam garantir a qualidade dos dados e sua transformação eficiente para responder a perguntas de negócio, além de praticar outros conhecimentos como o de Linux e o Git. Os arquivos do desafio final estão na pasta *scripts*

## Dependências

Nesta versão dos desafios finais da Aceleração Pyspark, utilizaremos scripts PySpark criado diretamente em python e versionar o código conforme as tarefas avançam. No momento não há outra dependência alé do [Spark instalado localmente](https://spark.apache.org/downloads.html).

## Inicializa os servidores Master e Worker do Spark
/opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://spark:7077

## Abra a interface web do Spark
firefox http://127.0.0.1:8080/ &

## Executa o script no Spark
spark-submit --master spark://spark:7077 capgemini-aceleracao-pyspark/scripts/<script>.py 2> /dev/null

ou ainda

spark-submit scripts/<script>.py 2> errs.txt > output.txt (sugestão de execução)

# Estrutura de diretórios

```
├── LICENSE
├── README.md
├── data                         <- Diretório contendo os dados brutos.
│   ├── airports.csv
│   ├── planes.csv
│   ├── flights.csv
|   ├── census-income
│       ├── census-income.csv
│       ├── census-income.names
|   ├── communities-crime
│       ├── communities-crime.csv
│       ├── communities.names
|   ├── online-retail
│       ├── online-retail.csv
│       ├── online-retail.names
│
├── scripts                    <- Contém respostas de negócio baseadas em dados.
│   ├── online-retail.py
│   ├── communities-crime.py
│   ├── census-income.py
├── notebooks                  <- Contém respostas do tratamento de qualidade dos dados.
│   ├── 1_quality.ipynb          <- Contém apontamentos de dados inconsistêntes.
│   ├── 2_transformation.ipynb   <- Contem tratamentos dos dados.
│   ├── 3_reports.ipynb          <- Contém respostas de negócio baseadas em dados.
```