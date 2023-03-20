# Airflow_tutorial_python
 airflow

I used Docker to run this application 

Will make sure Neo4j docker is running 

I ran with below command 


docker run \
    -p 7474:7474 -p 7687:7687 \
    --name neo4j-apoc \
    -e apoc.export.file.enabled=true \
    -e apoc.import.file.enabled=true \
    -e apoc.import.file.use_neo4j_config=false \
    -e NEO4J_PLUGINS=\[\"apoc\"\] \
    --env apoc.import.file.enabled=true \
    --env apoc.import.file.use_neo4j_config=false \
    --env NEO4J_AUTH=neo4j/password \
    neo4j:latest


We can load the data with apoc if we enable apoc.import.file.enabled=true
then 

CALL apoc.load.xml("file:///data.xml")
YIELD value
RETURN value

I created python script to load and Airflow Dag is used to automate 
<img width="1717" alt="Screenshot 2023-03-20 at 10 44 37" src="https://user-images.githubusercontent.com/42039695/226317439-68aaab68-62f8-4345-944f-79b4978e8e1f.png">



