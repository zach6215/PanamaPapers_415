from neo4j import GraphDatabase
import pprint

#Cole test URI = "neo4j+s://aa253cbe.databases.neo4j.io"  PASSWORD = "HSxe0WvTIH1XYojKVBePL05s8fro09AEznwXN-vc4Y4"
#Panama database = "neo4j+s://25b10ebc.databases.neo4j.io"  PASSWORD = "LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE"

#set password, username, and URI
NEO4J_URI = "neo4j+s://25b10ebc.databases.neo4j.io" #"neo4j+s://25b10ebc.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE" #"LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE"

#create driver and verify connectivity
URI = NEO4J_URI
AUTH = (NEO4J_USERNAME, NEO4J_PASSWORD)
driver = GraphDatabase.driver(URI, auth=AUTH)

driver.verify_connectivity()



def highest_degree(driver, typeOfNode = None):

     if typeOfNode != None:
          countVar = '{(n)--()}'
          query = f"MATCH (n: {typeOfNode}) RETURN n AS node, count{countVar} AS numberOfConnections ORDER BY numberOfConnections DESC LIMIT 10"
     elif typeOfNode == None:
          query = "MATCH (n) RETURN n AS node, count{(n)--()} AS numberOfConnections ORDER BY numberOfConnections DESC LIMIT 10"

     print(driver.execute_query(query))
     print("\n\n\n\n\n")


highest_degree(driver, "Entities")

highest_degree(driver)
