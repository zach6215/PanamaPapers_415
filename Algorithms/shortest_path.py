from neo4j import GraphDatabase
import pprint

#set password, username, and URI
NEO4J_URI = "neo4j+s://aa253cbe.databases.neo4j.io" #"neo4j+s://25b10ebc.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "HSxe0WvTIH1XYojKVBePL05s8fro09AEznwXN-vc4Y4" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE"

#create driver and verify connectivity
URI = NEO4J_URI
AUTH = (NEO4J_USERNAME, NEO4J_PASSWORD)
driver = GraphDatabase.driver(URI, auth=AUTH)

driver.verify_connectivity()

def shortest_path(driver, nodeA, nodeB):
     query = f"MATCH p = shortestPath((A)-[:LINK*]-(B)) WHERE A.name = '{nodeA}' AND B.name = '{nodeB}' RETURN [n in nodes(p) | n.name] AS stops"
     print(list(driver.execute_query(query)))

shortest_path(driver, "Bromsgrove", "Worcester Foregate Street")

