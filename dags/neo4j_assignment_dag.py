import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import xmltodict
from py2neo import Graph, Node, Relationship

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'provide_context': True,
}

dag = DAG(
    dag_id='neo4j_dag_assignment',
    default_args=args,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    schedule_interval='@daily')  # # Runs every hour between 06:00AM and 19:00PM


def Data_into_neo4j():
    print(f"Data started inserting")
    with open("data/Q9Y261.xml") as fd:
        data = xmltodict.parse(fd.read())
        data = data["uniprot"]["entry"]
    # Make sure neo4j database running at localhost

    graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"), name="neo4j")

    # if we want we can Delete all nodes and relationships in the graph
    graph.delete_all()

    # Started Creating nodes and relationships
    
    #[Node("Protein", accession=accession, name=data["name"]) for accession in data["accession"]]

    for accession in data["accession"]:
        protein_node = Node("Protein", accession=accession, name=data["name"])
        graph.create(protein_node)

        for name in data["protein"]["recommendedName"]["shortName"]:
            fullname_node = Node("FullName", name=name)
            graph.create(fullname_node)
            rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
            graph.create(rel)

        fullname_node = Node(
            "FullName", name=data["protein"]["recommendedName"]["fullName"]
        )
        graph.create(fullname_node)
        rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
        graph.create(rel)

        for alt_name in data["protein"]["alternativeName"]:
            if "fullName" in alt_name:
                fullname_node = Node("FullName", name=alt_name["fullName"])
                graph.create(fullname_node)
                rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
                graph.create(rel)

    # Create nodes and relationships for the features
    for feature in data["feature"]:
        feature_node = Node("Features", name=feature["@type"])
        graph.create(feature_node)
        rel = Relationship(protein_node, "HAS_FEATURE", feature_node)
        graph.create(rel)

    # Create nodes and relationships for the genes
    for gene_name in data["gene"]["name"]:
        gene_node = Node("Gene", name=gene_name["#text"], type=gene_name["@type"])
        graph.create(gene_node)

        rel = Relationship(protein_node, "ENCODES", gene_node)
        graph.create(rel)

    # Creating nodes and relationships for the organisms
    for organism in data["organism"]["name"]:
        organism_node = Node("Organism", name=organism["#text"])
        graph.create(organism_node)
        rel = Relationship(protein_node, "IN_ORGANISM", organism_node)
        graph.create(rel)
        for lineage in data["organism"]["lineage"]["taxon"]:
            lineage_node = Node("Lineage", name=lineage)
            graph.create(lineage_node)
            rel = Relationship(organism_node, "HAS_LINEAGE", lineage_node)
            graph.create(rel)

    # Creating nodes and relationships for the references
    for reference in data["reference"]:
        reference_node = Node("Reference", key=reference["@key"])
        graph.create(reference_node)

        rel = Relationship(protein_node, "HAS_REFERENCE", reference_node)
        graph.create(rel)

        citation = reference["citation"]
        citation_node = Node(
            "Citation",
            type=citation["@type"],
            volume=citation.get("@volume"),
            date=citation.get("@date"),
            first=citation.get("@first"),
            last=citation.get("@last"),
            title=citation["title"],
        )
        graph.create(citation_node)
        rel = Relationship(reference_node, "HAS_CITATION", citation_node)
        graph.create(rel)

        if "authorList" in citation:
            if "person" in citation["authorList"].keys():
                for author in citation["authorList"]["person"]:
                    author_node = Node("Author", name=author["@name"])
                    graph.create(author_node)
                    rel = Relationship(citation_node, "HAS_AUTHOR", author_node)
                    graph.create(rel)

        if "dbReference" in citation:
            for db_ref in citation["dbReference"]:
                db_ref_node = Node(
                    "DBReference", type=db_ref["@type"], id=db_ref["@id"]
                )

    print("All data Inserted into neo4j")

neo4j_data_insert = PythonOperator(
    task_id='neo4j_data_insert',
    python_callable=Data_into_neo4j,
    provide_context=True,
    retries=10,
    retry_delay=timedelta(seconds=1),
    dag=dag
    )

neo4j_data_insert