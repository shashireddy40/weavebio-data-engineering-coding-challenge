# Example code to load some data to Neo4j
# This code is based on the example code from the Neo4j Python Driver
# https://neo4j.com/docs/api/python-driver/current/

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import xmltodict
from py2neo import Graph, Node, Relationship
from datetime import datetime, timedelta

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()
        
    def creating_nodes_rel(self, path):
        print(f"Data started inserting")
        with open(path) as fd:
            data = xmltodict.parse(fd.read())
            Insert_data = data["uniprot"]["entry"]
        # Make sure neo4j database running at localhost

        with self.driver.session(database="neo4j") as session:
            graph = Graph(uri, auth=(user, password), name="neo4j")

        # if we want we can Delete all nodes and relationships in the graph
        #graph.delete_all()

        # we can do at cypher 
        # MATCH (n) DETACH DELETE n

        #

        for accession in Insert_data["accession"]:
            protein_node = Node("Protein", accession=accession, name=Insert_data["name"])
            graph.create(protein_node)

            for name in Insert_data["protein"]["recommendedName"]["shortName"]:
                fullname_node = Node("FullName", name=name)
                graph.create(fullname_node)
                rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
                graph.create(rel)

            fullname_node = Node(
                "FullName", name=Insert_data["protein"]["recommendedName"]["fullName"]
            )
            graph.create(fullname_node)
            rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
            graph.create(rel)

            for alt_name in Insert_data["protein"]["alternativeName"]:
                if "fullName" in alt_name:
                    fullname_node = Node("FullName", name=alt_name["fullName"])
                    graph.create(fullname_node)
                    rel = Relationship(protein_node, "HAS_FULLNAME", fullname_node)
                    graph.create(rel)

        # Create nodes and relationships for the features
        for feature in Insert_data["feature"]:
            feature_node = Node("Features", name=feature["@type"])
            graph.create(feature_node)
            rel = Relationship(protein_node, "HAS_FEATURE", feature_node)
            graph.create(rel)

        # Create nodes and relationships for the genes
        for gene_name in Insert_data["gene"]["name"]:
            gene_node = Node("Gene", name=gene_name["#text"], type=gene_name["@type"])
            graph.create(gene_node)

            rel = Relationship(protein_node, "ENCODES", gene_node)
            graph.create(rel)

        # Creating nodes and relationships for the organisms
        for organism in Insert_data["organism"]["name"]:
            organism_node = Node("Organism", name=organism["#text"])
            graph.create(organism_node)
            rel = Relationship(protein_node, "IN_ORGANISM", organism_node)
            graph.create(rel)
            for lineage in Insert_data["organism"]["lineage"]["taxon"]:
                lineage_node = Node("Lineage", name=lineage)
                graph.create(lineage_node)
                rel = Relationship(organism_node, "HAS_LINEAGE", lineage_node)
                graph.create(rel)

        # Creating nodes and relationships for the references
        for reference in Insert_data["reference"]:
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

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"
    app = App(uri, user, password)
    app.creating_nodes_rel("data/Q9Y261.xml")
    app.close()