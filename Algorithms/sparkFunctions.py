from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, expr, when, year

NEO4J_URI =  "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "dataGroup"


spark = SparkSession.builder \
    .appName("Neo4jExample") \
    .config("spark.jars", r"C:\Users\colec\Downloads\neo4j-connector-apache-spark_2.12-5.2.0_for_spark_3.jar") \
    .getOrCreate()

#takes a string cypher query and returns a spark dataframe
def getSparkDF(query):
    global NEO4J_URI
    global NEO4J_PASSWORD
    global NEO4J_USERNAME
    global spark

    df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", NEO4J_URI) \
        .option("authentication.basic.username", NEO4J_USERNAME) \
        .option("authentication.basic.password", NEO4J_PASSWORD) \
        .option("query", query) \
        .load()

    return df

#returns a spark dataframe with all nodes that are directly connected to the input node. If a node is between them the end node will not be returned
def sparkConnected(nodeName):
    return getSparkDF(f"MATCH ((a)-[r]-(b)) WHERE a.name = \"{nodeName}\" RETURN b.name AS connected_node, TYPE(r) AS connection_type")

#takes a number and returns a pyspark dataframe of the countries (excluding the U.S.) and the number of times they appear in the corresponding label.
#put one in for Entity, 2 for Intermediary, or any other number for Officer
#since this data has been reduced to U.S. nodes and their connections this shows how many U.S. connections each country has in the Panama Papers
def countries(type):
    if type == 1:
        label = "Entity"
    elif type == 2:
        label = "Intermediary"
    else:
        label = "Officer"

    df = getSparkDF(f"MATCH (n:{label}) WHERE NOT n.countries = 'United States' RETURN n.name AS {label}_name, n.countries AS {label}_country")
    country_count = df.groupby(f"{label}_country").count()
    country_count = country_count.orderBy(col("count").desc())
    return country_count

#takes the names of a start node and end node
#returns the shortest path between them as a list of node names
def newShortestPath(startNode, endNode):
    temp = getSparkDF(f"MATCH p = shortestPath((a {{name: '{startNode}'}})-[*]-(b {{name: '{endNode}'}})) RETURN [node IN nodes(p) | node.name] AS node_names")
    return temp.first()["node_names"]


def timeActive(order='d', remove_null='t'):
    temp = getSparkDF("MATCH (n:Entity) RETURN n.name AS Entity_name, n.countries AS Entity_country, n.incorporation_date AS Entity_incorporation_date, n.inactivation_date AS Entity_inactivation_date")
    temp = temp.withColumn("Entity_incorporation_date", to_date(col("Entity_incorporation_date"), "d-MMM-yy"))
    temp = temp.withColumn("Entity_inactivation_date", to_date(col("Entity_inactivation_date"), "d-MMM-yy"))
    temp = temp.withColumn("Entity_active_time", datediff(col("Entity_inactivation_date"), col("Entity_incorporation_date")))

    if remove_null == 't':
        temp = temp.dropna()

    if order == 'a':
        temp = temp.orderBy("Entity_active_time")
    else:
        temp = temp.orderBy("Entity_active_time", ascending=False)

    return temp

# time_active = timeActive()
# time_active.show(truncate=False)

# shortestPath = newShortestPath("DANRESA LIMITED", "SEDGEFIELD LTD.")
# print(shortestPath)

# cc = countries(1)
# cc.show(truncate=False)

# nodes = sparkConnected("TRANSFORWARD N.V.")
# nodes.show(truncate=False)

spark.stop()