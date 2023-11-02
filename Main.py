from neo4j import GraphDatabase

if __name__ == '__main__':
    NEO4J_URI = "neo4j+s://25b10ebc.databases.neo4j.io"
    NEO4J_USERNAME = "neo4j"
    NEO4J_PASSWORD = "LuxCPqYg2HjZG4GN_-UVte3pUXtpAI7t-8qMvBBuijE"

    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = NEO4J_URI
    AUTH = (NEO4J_USERNAME, NEO4J_PASSWORD)

    driver = GraphDatabase.driver(URI, auth=AUTH)

    driver.verify_connectivity()

    