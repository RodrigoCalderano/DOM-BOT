# Sistema Automatizado para Operação na Bolsa de Valores

Este projeto tem como objetivo:

* Tratar valores históricos de forma a gerar um banco de dados com indicadores necessários para o projeto.
* Aprimorar estratégias de operação na bolsa de valores baseadas em análise técnica buscando otimização de parâmetros a partir de backtesting.
* Automatizar operações em tempo real utlizando MetaTrader e um Bot do Telegram para monitoramento.

## Arquitetura
A arquitetura proposta é baseada em um sistema que consome um banco de valores históricos, coleta dados em tempo real pelo MetaTrader e, após um tratamento, estes dados são enviados por meio de filas assíncronas que são consumidas pelos módulos que representam as estratégias.

![alt text](Drawables/diagrama_vetor.png "Diagrama do sistema proposto")

**MAD** - Módulo de aquisição de dados: responsável por obter os dados do MetaTrader e do banco de dados.

**Filas de entrada** : Rotinas de processamento de dados.

**Bollinger Bands//Long & Shor** -  Módulos de execução de estratégias.

**MET** -  Módulo de execução de tarefas: responsável por enviar ordens de operações para o MetaTrader e avisos pelo Telegram.

## Conexão MetaTrader ~ Python
* metatrader2python.mq5 é o responsável por enviar e receber informações para o sistema em Python por meio de socket local.
