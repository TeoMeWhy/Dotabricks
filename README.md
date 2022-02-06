# Dotabricks

Lives criando um Datalake com dados de Dota2 na plataforma [Databricks](http://databricks.com/). Basta acessar: [twitch.tv/teomewhy](twitch.tv/teomewhy)

<img src="https://i.ibb.co/FgTsVPv/Solu-es-Data-Lake-Frame-6.jpg" alt="" width="550">

## Motivação

Nossa missão é transmitir conhecimento com exemplo práticos e claros ao maior número de pessoas possível. Quem sabe, por consequência, podemos mudar este mundo para melhor.

Apesar de minha especialidade não ser engenharia de dados, tenho grande interesse pelo tema, sendo um grande entusiasta. Aqui, serei provocado a utilizar o que há de mais novo sobre o tema de datalakes e lakehouse, e gostaria muito que me acompanhasse nesta jornada.

## Por que Datalake?

Este conceito nos permite armazenar, processar e analisar um grande conjunto de dados de diferentes formatos e estruturas. Além de ser uma tecnologia mais barata que outras soluções, como os bancos de dados relacionais. Vale considerar ainda a possibilidade (praticamente infinita) de escalar a infraestrutura de forma horizontal e vertical, isto é, adicionado mais máquinas (e maiores) ao seu cluster de processamento de Apache Spark.

## Por que Databricks?

Gostaria de elencar as principais vantagens do meu ponto de vista ao utilizar o Databricks no lugar de outra solução, seja open-source ou serviço.

1. Abstração da arquitetura. Este serviço é praticamente um plug-and-play para começar suas ingestões de dados, exigindo o mínimo de setup inicial. Há a possibilidade da criação de workspace no modo quickstarter, onde basicamente é criada uma pilha no CloudFormation e basta acionar um botão para subir toda infra estrutura necessária. Caso isso não seja o melhor caminho, é totalmente possível realizar este setup de forma customizada no modelo avançado.
2. Setup e gestão do Apache Spark. Ao escolher a abordagem open-source, devemos nos preocupar com diversos fatores: versão do Apache Spark, versão do Delta, manejo dos arquivos jars, criação de sessões Spark customizadas, etc. Ao utilizar esta solução como serviço, tudo isso também é abstraído, mantendo a possibilidade de você adicionar configurações adicionais caso necessário. Vale dizer sobre a parte de desligamento automático dos cluster para economia de recursos caso o cluster esteja ocioso.
3. Solução end-to-end. A plataforma fornece mais do que apenas um cluster Spark e notebooks, podemos utilizar seu próprio agendador de Jobs e orquestrador de Tasks. Bem como definir acessos às tabelas e schemas em nível de usuário e grupos.
4. Não há necessidade da criação de ambiente local. Ao se adotar outras opções no mercado, é comum se fazer necessário a criação de um ambiente de desenvolvimento local, precisando se aproximar muito do que está em produção ou homologação. Supondo que utilizamos Apache Airflow on K8S, será necessário ter localmente um ambiente com K8S e Airflow rodando. Isso gera a necessidade de todos que gostariam de criar Dags realizar este setup em seus notebooks.
5. Integração com repositórios Git. A própria solução permite este tipo de integração de forma extramente facilitada. É muito simples configurar e clonar repositório para que seu workspace sincronize os códigos de produção.
6. Apache Spark e Delta otimizados. Os fundadores do Databricks são criadores do Apache Spark e Delta Lake que estão disponíveis em formato open-source. Mas, a empresa garante que algumas features são exclusivas para sua plataforma, bem como terem uma versão do Apache Spark otimizada. Você pode conferir [aqui uma comparação de features](https://databricks.com/spark/comparing-databricks-to-apache-spark).

Com base nessas evidências coletadas durante algumas provas de conceito e por amor à simplicidade, entendo que seria uma ótima ferramenta para ser apresentada em minhas lives. Além e claro, da vasta adoção do mercado.

## Como vamos tocar o projeto?

Nossas lives acontecerão aos sábados às 15:00 no canal [twitch.tv/teomewhy](twitch.tv/teomewhy). A ideia é avançarmos nas ingestões de dados, bem como organização de nosso datalake/lakehouse.
A live é aberta para todo mundo participar e contribuir com o projeto, bem como tirar dúvidas.

**IMPORTANTE:** Isso não é um curso, e sim live coding. Ou seja, não temos um caminho formal de como as coisas vão acontecer, estarei descobrindo junto com você como lidar com problemas e buscar soluções.

## É de graça?

Sim, você pode não paga absolutamente nada para participar de nossa live. Mas caso deseje rever o conteúdo, é necessário ser Sub no canal da Twitch.
Hoje há duas formas de você ser sub no canal:
1. Amazon Prime: Caso você seja assinante da Amazon Prime, todo mês você pode escolher um streamer da Twitch para se inscrever no canal. Para fazer essa configuração, basta [clicar aqui](https://twitch.amazon.com/tp) e depois se inscrever no canal com o Prime.
2. Pagar mensalmente a assinatura ou fazer um plano maior. Hoje a assinatura está R$7,90/mês.

## Por que ser um inscrito no canal?

Como disse, o conteúdo é gratuíto, mas você pode rever os vídeos sendo um inscrito. Além, é claro, de apoiar este nsso projeto de fomentar o conhecimento.

Para realizar um projeto destes, há custos envolvidos para manter todo ambiente cloud. Assim, os valores adquiridos pelas assinaturas são utilizados para sanar estes custos.
