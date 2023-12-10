from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, expr, when, year, udf
import matplotlib.pyplot as plt
import networkx as nx
import time
from IPython.display import display

NEO4J_URI =  "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "dataGroup"
roundNumber = 3


spark = SparkSession.builder \
    .appName("Neo4jExample") \
    .config("spark.jars", r"C:\Users\colec\Downloads\neo4j-connector-apache-spark_2.12-5.2.0_for_spark_3.jar") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

#takes a string cypher query and returns a spark dataframe
def getSparkDF(query):
    t1 = time.perf_counter()
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

    t2 = time.perf_counter()
    print(f"total time elapsed getSparkDF({query}):", round((t2 - t1), roundNumber))

    return df

#returns a spark dataframe with all nodes that are directly connected to the input node. If a node is between them the end node will not be returned
def sparkConnected(nodeName):
    t1 = time.perf_counter()

    temp = getSparkDF(f"MATCH ((a)-[r]-(b)) WHERE a.name = \"{nodeName}\" RETURN b.name AS connected_node, TYPE(r) AS connection_type")

    pandas_df = temp.toPandas()
    ax = plt.subplot()
    ax.axis('off')
    table = ax.table(cellText=pandas_df.values, colLabels=pandas_df.columns, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 2)

    t2 = time.perf_counter()
    print(f"total time elapsed sparkConnected({nodeName}):", round((t2 - t1), roundNumber))

    plt.show()

#takes a number and returns a pyspark dataframe of the countries (excluding the U.S.) and the number of times they appear in the corresponding label.
#put one in for Entity, 2 for Intermediary, or any other number for Officer
#since this data has been reduced to U.S. nodes and their connections this shows how many U.S. connections each country has in the Panama Papers
def countries(type):
    t1 = time.perf_counter()

    if type == 1:
        label = "Entity"
        label2 = "Entities"
    elif type == 2:
        label = "Intermediary"
        label2 = "Intermediaries"
    else:
        label = "Officer"
        label2 = "Officers"

    df = getSparkDF(f"MATCH (n:{label}) WHERE NOT n.countries = 'United States' RETURN n.name AS {label}_name, n.countries AS {label}_country")
    country_count = df.groupby(f"{label}_country").count()
    country_count = country_count.orderBy(col("count").desc())

    cleanerUDF = udf(lambda string: string.replace("United States;", ""))
    if type == 2:
        country_count = country_count.withColumn(label + '_country', cleanerUDF(country_count.Intermediary_country))

    pandas_df = country_count.toPandas()
    plt.figure(figsize=(11, 8))
    plt.bar(pandas_df[label + '_country'], pandas_df['count'])
    plt.title('Countries With The Most Connections To U.S. ' + label2)
    plt.xlabel('Countries')
    plt.ylabel('Number of Connections')
    plt.xticks(rotation='vertical')

    t2 = time.perf_counter()
    print(f"total time elapsed countries({type}):", round((t2 - t1), roundNumber))

    plt.show()

#takes the names of a start node and end node
#returns the shortest path between them as a list of node names
def newShortestPath(startNode, endNode):
    t1 = time.perf_counter()

    temp = getSparkDF(f"MATCH p = shortestPath((a {{name: '{startNode}'}})-[*]-(b {{name: '{endNode}'}})) RETURN [node IN nodes(p) | node.name] AS node_names")

    t2 = time.perf_counter()
    print(f"total time elapsed newShortestPath({startNode},{endNode}):", round((t2 - t1), roundNumber))

    showPath(temp.first()["node_names"])

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


def timeActive(order='d', remove_null='t'):
    t1 = time.perf_counter()

    temp = getSparkDF("MATCH (n:Entity) RETURN n.name AS Entity_name, n.countries AS Entity_country, n.incorporation_date AS Entity_incorporation_date, n.inactivation_date AS Entity_inactivation_date")

    def fixDate(date):
        if date == None:
            return None
        dateList = date.split('-')
        if int(dateList[2]) > 20:
            dateList[2] = "19" + dateList[2]
        else:
            dateList[2] = "20" + dateList[2]
        return dateList[0] + "-" + dateList[1] + "-" + dateList[2]

    fixDateUDF = udf(fixDate)
    temp = temp.withColumn("Entity_incorporation_date", fixDateUDF(temp.Entity_incorporation_date))
    temp = temp.withColumn("Entity_inactivation_date", fixDateUDF(temp.Entity_inactivation_date))

    temp = temp.withColumn("Entity_incorporation_date", to_date(col("Entity_incorporation_date"), "d-MMM-yyyy"))
    temp = temp.withColumn("Entity_inactivation_date", to_date(col("Entity_inactivation_date"), "d-MMM-yyyy"))
    temp = temp.withColumn("Entity_active_time", datediff(col("Entity_inactivation_date"), col("Entity_incorporation_date")))

    if remove_null == 't':
        temp = temp.dropna()

    if order == 'a':
        temp = temp.orderBy("Entity_active_time")
    else:
        temp = temp.orderBy("Entity_active_time", ascending=False)

    pandas_df = temp.toPandas()
    pandas_df = pandas_df.head(20)
    pandas_df["Entity_name"] = pandas_df["Entity_name"].str[:5]
    plt.figure(figsize=(11, 8))
    plt.bar(pandas_df['Entity_name'], pandas_df['Entity_active_time'])
    plt.title('Entity Active Time')
    plt.xlabel('Entities')
    plt.ylabel('Days Active')
    plt.xticks(rotation='vertical')

    t2 = time.perf_counter()
    print(f"total time elapsed timeActive({order}, {remove_null}):", round((t2 - t1), roundNumber))

    plt.show()

#timeActive()
#newShortestPath("DANRESA LIMITED", "SEDGEFIELD LTD.")
#countries(3)
#sparkConnected("TRANSFORWARD N.V.")
