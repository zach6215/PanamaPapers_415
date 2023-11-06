import networkx as nx
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!WARNING THESE KEYS AND INPUTS ARE FOR A TEST DATABASE NOT THE PANAMA PAPERS DATABASE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#set password, username, and URI
NEO4J_URI =  "neo4j+s://aa253cbe.databases.neo4j.io" #"neo4j+s://25b10ebc.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "HSxe0WvTIH1XYojKVBePL05s8fro09AEznwXN-vc4Y4" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE"

#create driver and verify connectivity (exception is thrown when there is no connection, no exception otherwise)
URI = NEO4J_URI
AUTH = (NEO4J_USERNAME, NEO4J_PASSWORD)
driver = GraphDatabase.driver(URI, auth=AUTH)

driver.verify_connectivity()

#showPath algorithm creates and displays a graph of the path between two nodes
# list is a list of nodes in the path in order
def showPath(list):
    G = nx.DiGraph()
    lastNode = ""

    for node in list:
        G.add_node(node)
        if (lastNode != ""):
            G.add_edge(lastNode, node)
        lastNode = node

    pos = nx.spring_layout(G)

    nx.draw_networkx_nodes(G, pos)
    nx.draw_networkx_labels(G, pos)
    nx.draw_networkx_edges(G, pos, edge_color='b', arrows = True)

    plt.show()

#shortest_path takes two node names and GraphDatabase.driver
#returns the shortest path between the nodes as a list of node names
def shortest_path(driver, nodeA, nodeB):
    query = f"MATCH p = shortestPath((A)-[:LINK*]-(B)) WHERE A.name = '{nodeA}' AND B.name = '{nodeB}' RETURN [n in nodes(p) | n.name] AS stops"
    result = driver.execute_query(query).records
    return list(result[0]['stops'])

showPath(shortest_path(driver, "Bromsgrove", "Worcester Foregate Street"))
