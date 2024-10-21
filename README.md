# BEES Data Engineering – Breweries Case

## Objetivo

O objetivo deste projeto é demonstrar habilidades em consumir dados de uma API, transformá-los e persistir em um data lake seguindo a arquitetura medallion, com três camadas: dados brutos, dados curados particionados por localização e uma camada analítica agregada.

## Arquitetura e Ferramentas Utilizadas

- **API**: Open Brewery DB para a listagem de cervejarias. Endpoint utilizado:
  ```
  https://api.openbrewerydb.org/breweries
  ```

- **Ferramenta de Orquestração**: Escolhi o **Airflow** para construir o pipeline de dados, devido à sua capacidade de lidar com agendamentos, tentativas de repetição, e tratamento de erros de forma eficiente.

- **Linguagem**: Utilizei **Python** para requisições e transformação dos dados, integrando o uso do **PySpark** para processamento de dados em grande escala.

- **Containerização**: O projeto foi modularizado utilizando **Docker**, facilitando o ambiente de desenvolvimento e execução dos pipelines em containers isolados.

- **Camadas do Data Lake**:
  - **Bronze (Raw Data)**: Dados da API são armazenados no formato JSON sem transformações.
  - **Silver (Curated Data)**: Dados transformados e particionados por localização, armazenados no formato Parquet.
  - **Gold (Analytical Layer)**: Uma visão agregada contendo a quantidade de cervejarias por tipo e localização.

## Execução

### Pré-requisitos

1. **Docker**: Certifique-se de ter o Docker instalado. Caso contrário, você pode instalar seguindo as instruções [aqui](https://docs.docker.com/get-docker/).
2. **Docker Compose**: Instalado junto com o Docker Desktop.

### Como rodar o projeto

1. Clone este repositório:
   ```bash
   git clone https://github.com/AlexandreFCosta/desafio_abinbev.git
   cd desafio_abinbev
   ```

2. Crie o arquivo `.env` para definir as variáveis de ambiente necessárias, como as credenciais do Gmail para o envio de alertas.

3. Inicie o ambiente Docker:
   ```bash
   docker-compose up
   ```

4. Acesse a interface do Airflow:
   - Abra seu navegador e vá até `http://localhost:8080`
   - Use as credenciais padrão `airflow` para login.

5. Execute o pipeline diretamente pela interface do Airflow.

### Monitoramento e Alertas

- **Monitoramento**: Utilizei o Airflow para monitoramento dos DAGs, configurando retries automáticos para lidar com possíveis falhas no pipeline.
- **Alertas**: Para notificações de falhas ou erros críticos no pipeline, configurei alertas via **Gmail**, que enviam um e-mail caso ocorra uma falha.

### Testes

Testes foram incluídos para validar:
- Consumo da API
- Transformação e particionamento dos dados
- Agregações para a camada Gold

## Considerações Finais

O pipeline foi projetado com foco na modularidade e escalabilidade. Além disso, medidas de resiliência foram tomadas, como retries automáticos e alertas configurados via Gmail. 
