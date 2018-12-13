import click
from google.cloud import bigquery
import pandas as pd

uni1 = 'jw3696' # Your uni
uni2 = 'hz2562' # Partner's uni. If you don't have a partner, put None


# Test function
def testquery(client):
	q = """select * from `w4111-columbia.graph.tweets` limit 3"""
	job = client.query(q)

	# waits for query to execute and return
	results = job.result()
	return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
	q1 = """SELECT id, text 
			FROM `w4111-columbia.graph.tweets` 
			WHERE text LIKE \'%going live%\' AND text LIKE \'%www.twitch%\'"""
	job1 = client.query(q1)

	result1 = job1.result()
	return list(result1)
	#return []

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
	q2 = """SELECT SUBSTR(create_time,1,3) AS day, avg(like_num) AS avg_likes 
			FROM `w4111-columbia.graph.tweets` 
			GROUP BY day 
			ORDER BY avg_likes desc
			LIMIT 1"""
	job2 = client.query(q2)

	result2 = job2.result()
	return list(result2)
	#return []

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
	q3 = """SELECT twitter_username AS src, REGEXP_EXTRACT(text, "@([a-zA-Z0-9_.+-]+)") AS dst
			FROM `w4111-columbia.graph.tweets`
			WHERE text LIKE \'%@%\' 
			GROUP BY src,dst"""

	sql = """select twitter_username as src, REGEXP_EXTRACT(text, "@([a-zA-Z0-9_.+-]+)") AS dst from [w4111-columbia.graph.tweets] \
			where text like \'%@%\' group by src,dst"""
	save(sql)

	job3 = client.query(q3)

	result3 = job3.result()
	return list(result3)
	#return []

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
	q4 = """
	WITH INDEGREE AS(
	SELECT COUNT(src) AS max_indegree, ROW_NUMBER() OVER (ORDER BY COUNT(src) DESC) AS rank
	FROM dataset1.GRAPH
	GROUP BY dst
	ORDER BY COUNT(src) DESC
	LIMIT 1),

	OUTDEGREE AS(
	SELECT COUNT(dst) AS max_outdegree, ROW_NUMBER() OVER (ORDER BY COUNT(dst) DESC) AS rank
	FROM dataset1.GRAPH
	GROUP BY src
	ORDER BY COUNT(dst) DESC
	LIMIT 1)

	SELECT I.max_indegree, O.max_outdegree
	FROM INDEGREE AS I JOIN OUTDEGREE AS O ON I.rank = O.rank
	"""
	job4 = client.query(q4)

	result4 = job4.result()
	return list(result4)
	#return []

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
	q = """
	WITH AVGLIKE AS(
	SELECT twitter_username, AVG(like_num) AS avg
	FROM `w4111-columbia.graph.tweets`
	GROUP BY twitter_username
	),

	INDEGREE AS(
	SELECT dst, COUNT(src) AS count
	FROM dataset1.GRAPH
	GROUP BY dst
	),

	POPULAR AS(
	SELECT twitter_username
	FROM `w4111-columbia.graph.tweets` 
	WHERE twitter_username IN (SELECT twitter_username FROM AVGLIKE WHERE avg >= (SELECT AVG(avg) FROM AVGLIKE)) AND
	twitter_username IN (SELECT dst FROM INDEGREE WHERE count >= (SELECT AVG(count) FROM INDEGREE))
	),

	UNPOPULAR AS(
	SELECT twitter_username
	FROM `w4111-columbia.graph.tweets`	WHERE twitter_username IN (SELECT twitter_username FROM AVGLIKE WHERE avg < (SELECT AVG(avg) FROM AVGLIKE)) AND
	twitter_username IN (SELECT dst FROM INDEGREE WHERE count < (SELECT AVG(count) FROM INDEGREE))
	),

	SAMPLE AS(
	SELECT *
	FROM dataset1.GRAPH
	WHERE src IN (SELECT twitter_username FROM UNPOPULAR)
	)

	SELECT SAFE_DIVIDE((SELECT COUNT(*) FROM SAMPLE WHERE dst IN (SELECT twitter_username FROM POPULAR)),
	(SELECT COUNT(*) FROM SAMPLE)) AS popular_unpopular
	"""

	job = client.query(q)

	result = job.result()

	df = job.to_dataframe()
	print(df.head(10))

	return list(result)
	#return []

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
	# Query Explanation:
	# Table TRIANGLE: get all the triangles, there might be duplicates like: 1, 2, 3 and 3, 2, 1
	# Table DISTINCT_TRIANGLE: manually sort the three points of the triangle and make sure that the a1 < a2 < a3, so
	#						   the collection of the triangles must be unique
	# Calculate the count of distinct triangles and rename the output
 
	q = """
	WITH TRIANGLE AS(
	SELECT DISTINCT g1.src AS n1, g2.src AS n2, g3.src AS n3
	FROM dataset1.GRAPH AS g1 JOIN dataset1.GRAPH AS g2 ON g1.dst = g2.src JOIN dataset1.GRAPH AS g3 ON g2.dst = g3.src
	WHERE g1.src = g3.dst AND g1.src != g2.src AND g2.src != g3.src AND g3.src != g1.src
	),
	
	DISTINCT_TRIANGLE AS(
	SELECT DISTINCT a1, a2, a3
	FROM (
	SELECT n1 AS a1, n2 AS a2, n3 AS a3 
	FROM TRIANGLE
	WHERE n1 < n2 AND n2 < n3

	UNION ALL
	
	SELECT n1 AS a1, n3 AS a2, n2 AS a3
	FROM TRIANGLE
	WHERE n1 < n3 AND n3 < n2

	UNION ALL

	SELECT n2 AS a1, n1 AS a2, n3 AS a3
	FROM TRIANGLE
	WHERE n2 < n1 AND n1 < n3

	UNION ALL

	SELECT n2 AS a1, n3 AS a2, n1 AS a3
	FROM TRIANGLE
	WHERE n2 < n3 AND n3 < n1

	UNION ALL

	SELECT n3 AS a1, n1 AS a2, n2 AS a3
	FROM TRIANGLE
	WHERE n3 < n1 AND n1 < n2

	UNION ALL

	SELECT n3 AS a1, n2 AS a2, n1 AS a3
	FROM TRIANGLE
	WHERE n3 < n2 AND n2 < n1
	))

	SELECT COUNT(*) AS no_of_triangles
	FROM DISTINCT_TRIANGLE
	"""

	job = client.query(q)

	result = job.result()
	return list(result)

	#return []

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):

	

	return []


# PageRank algorithm.
def pageRank(client, n_iter):
	q = """
	CREATE OR REPLACE TABLE dataset1.PAGERANK (node string, output numeric, currrank numeric, nextrank numeric) AS
	SELECT n.node as node, g.output AS output, 1 AS currank, 0 AS nextrank 
	FROM 
  (SELECT DISTINCT node
	FROM(
	SELECT DISTINCT src AS node
	FROM dataset1.GRAPH

	UNION ALL

	SELECT DISTINCT dst AS node
	FROM dataset1.GRAPH
	)) AS n 
  
  LEFT OUTER JOIN 
  
  (SELECT src AS node, SAFE_DIVIDE(1,COUNT(*)) AS output
	FROM dataset1.GRAPH
	GROUP BY src
	) AS g ON n.node = g.node
	"""
































	q100 = """
	WITH NODELIST AS(
	SELECT DISTINCT node
	FROM(
	SELECT DISTINCT src AS node
	FROM dataset1.GRAPH

	UNION ALL

	SELECT DISTINCT dst AS node
	FROM dataset1.GRAPH
	)),

	GOOUT AS(
	SELECT src AS node, SAFE_DIVIDE(1,COUNT(*)) AS output
	FROM dataset1.GRAPH
	GROUP BY src
	)

	SELECT n.node as node, g.output AS output, 1 AS currank, 0 AS nextrank 
	FROM NODELIST AS n LEFT OUTER JOIN GOOUT AS g ON n.node = g.node 
	"""

	q100 ="""
	SELECT DISTINCT COUNT(twitter_username)
	FROM 

	"""

	job = client.query(q)
	results = job.result()
	pageRank = job.to_dataframe()

	# Table PageRank:
	#		node string: the name of the node
	#		currank numeric: the current rank of the node
	#		output numeric: the weight to transfer to each dst
	#		nextrank: the rank of the next iteration


CREATE OR REPLACE TABLE dataset1.PAGERANK (node string, output numeric, currrank numeric, nextrank numeric) AS
	SELECT n.node as node, CAST(g.output AS numeric) AS output, 1 AS currank, 0 AS nextrank 
	FROM 
  (SELECT DISTINCT node
	FROM(
	SELECT DISTINCT src AS node
	FROM dataset1.GRAPH

	UNION ALL

	SELECT DISTINCT dst AS node
	FROM dataset1.GRAPH
	)) AS n 
  
  LEFT OUTER JOIN 
  
  (SELECT src AS node, SAFE_DIVIDE(1,COUNT(*)) AS output
	FROM dataset1.GRAPH
	GROUP BY src
	) AS g ON n.node = g.node;
  


SELECT name, O.out AS out, 1/6292 AS currRank, 0 AS nextRank
FROM (
  SELECT DISTINCT(twitter_username) AS name 
  FROM `w4111-columbia.graph.tweets` 
  GROUP BY twitter_username) AS node FULL JOIN (
  SELECT src AS srcNode , 1/COUNT(*) AS out
  FROM dataset1.GRAPH
  GROUP BY src) AS O ON node.name = O.srcNode





















	q1 = """
		CREATE OR REPLACE TABLE dataset1.PAGERANK (node string, currrank FLOAT64, output FLOAT64, nextrank FLOAT64) AS
		SELECT n.node as node, g.output AS output, 0 AS currank, 0 AS nextrank
		FROM 	(SELECT DISTINCT node
				FROM(
				SELECT DISTINCT src AS node
				FROM dataset1.GRAPH

				UNION ALL

				SELECT DISTINCT dst AS node
				FROM dataset1.GRAPH
				)) AS n 

		LEFT OUTER JOIN 
				
				(SELECT src AS node, SAFE_DIVIDE(1,COUNT(*)) AS output
				FROM dataset1.GRAPH
				GROUP BY src) AS g 
		
		ON n.node = g.node 
		"""
	q2 = """
	SELECT *
	FROM dataset1.PAGERANK
	WHERE output != 1.0 AND outp
	ORDER BY output
	LIMIT 20
	"""










	q3 = """
		INSERT INTO PAGERANK
		WITH NODELIST AS(
		SELECT DISTINCT node, SAFE_DIVIDE(1, COUNT(*)) AS currank
		FROM(
		SELECT DISTINCT src AS node
		FROM dataset1.GRAPH

		UNION ALL

		SELECT DISTINCT dst AS node
		FROM dataset1.GRAPH
		)),

		GOOUT AS(
		SELECT src AS node, SAFE_DIVIDE(1,COUNT(*)) AS output
		FROM dataset1.GRAPH
		GROUP BY src
		)

		SELECT n.node as node, g.output AS output, 1 AS currank, 0 AS ne 
		FROM NODELIST AS n LEFT OUTER JOIN GOOUT AS g ON n.node = g.node 
		"""

	"""
		SELECT n.node, SAFE_DIVIDE(1, COUNT(*)) AS currank, g.output, 0 AS nextrank
		FROM NODELIST AS n LEFT OUTER JOIN GOOUT AS g
		ON n.node = g.node;
	"""
	#INSERT INTO dataset1.PAGERANK(node, currrank, output, nextrank) VALUES

	q3 = """
	SELECT *
	FROM dataset1.PAGERANK
	LIMIT 20
	"""

	



	'''
	# You should replace dataset.distances with your dataset name and table name. 
	q3 = """
		CREATE OR REPLACE TABLE dataset1.PAGERANK AS
		SELECT '{start}' as node, 0 as distance
		""".format(start=start)
	
	job = client.query(q3)
	# Result will be empty, but calling makes the code wait for the query to complete
	job.result()

	for i in range(n_iter):
		print("Step %d..." % (i+1))
		q1 = """
		INSERT INTO dataset.distances(node, distance)
		SELECT distinct dst, {next_distance}
		FROM dataset.bfs_graph
			WHERE src IN (
				SELECT node
				FROM dataset.distances
				WHERE distance = {curr_distance}
				)
			AND dst NOT IN (
				SELECT node
				FROM dataset.distances
				)
			""".format(
				curr_distance=i,
				next_distance=i+1
			)
		job = client.query(q1)
		results = job.result()
		# print(results)
	'''



# Do not edit this function. This is for helping you develop your own iterative PageRank algorithm.
def bfs(client, start, n_iter):

	# You should replace dataset.bfs_graph with your dataset name and table name.
	q1 = """
		CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
		"""
	q2 = """
		INSERT INTO dataset.bfs_graph(src, dst) VALUES
		('A', 'B'),
		('A', 'E'),
		('B', 'C'),
		('C', 'D'),
		('E', 'F'),
		('F', 'D'),
		('A', 'F'),
		('B', 'E'),
		('B', 'F'),
		('A', 'G'),
		('B', 'G'),
		('F', 'G'),
		('H', 'A'),
		('G', 'H'),
		('H', 'C'),
		('H', 'D'),
		('E', 'H'),
		('F', 'H');
		"""

	job = client.query(q1)
	results = job.result()
	job = client.query(q2)
	results = job.result()

	# You should replace dataset.distances with your dataset name and table name. 
	q3 = """
		CREATE OR REPLACE TABLE dataset.distances AS
		SELECT '{start}' as node, 0 as distance
		""".format(start=start)
	job = client.query(q3)
	# Result will be empty, but calling makes the code wait for the query to complete
	job.result()

	for i in range(n_iter):
		print("Step %d..." % (i+1))
		q1 = """
		INSERT INTO dataset.distances(node, distance)
		SELECT distinct dst, {next_distance}
		FROM dataset.bfs_graph
			WHERE src IN (
				SELECT node
				FROM dataset.distances
				WHERE distance = {curr_distance}
				)
			AND dst NOT IN (
				SELECT node
				FROM dataset.distances
				)
			""".format(
				curr_distance=i,
				next_distance=i+1
			)
		job = client.query(q1)
		results = job.result()
		# print(results)


# Do not edit this function. You can use this function to see how to store tables using BigQuery.
def save_table():
	client = bigquery.Client()
	dataset_id = 'dataset'

	job_config = bigquery.QueryJobConfig()
	# Set use_legacy_sql to True to use legacy SQL syntax.
	job_config.use_legacy_sql = True
	# Set the destination table
	table_ref = client.dataset(dataset_id).table('test')
	job_config.destination = table_ref
	job_config.allow_large_results = True
	sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

	# Start the query, passing in the extra configuration.
	query_job = client.query(
		sql,
		# Location must match that of the dataset(s) referenced in the query
		# and of the destination table.
		location='US',
		job_config=job_config)  # API request - starts the query

	query_job.result()  # Waits for the query to finish
	print('Query results loaded to table {}'.format(table_ref.path))

def save(sql):
	client = bigquery.Client()
	dataset_id = 'dataset1'

	job_config = bigquery.QueryJobConfig()
	# Set use_legacy_sql to True to use legacy SQL syntax.
	job_config.use_legacy_sql = True
	# Set the destination table
	table_ref = client.dataset(dataset_id).table('GRAPH')
	job_config.destination = table_ref
	job_config.allow_large_results = True
	#sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

	# Start the query, passing in the extra configuration.
	query_job = client.query(
		sql,
		# Location must match that of the dataset(s) referenced in the query
		# and of the destination table.
		location='US',
		job_config=job_config)  # API request - starts the query

	query_job.result()  # Waits for the query to finish
	print('Query results loaded to table {}'.format(table_ref.path))

@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
	client = bigquery.Client.from_service_account_json(pathtocred)

	#funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
	funcs_to_test = [q7]
	#funcs_to_test = [testquery]
	for func in funcs_to_test:
		rows = func(client)
		print ("\n====%s====" % func.__name__)
		print(rows)

	#bfs(client, 'A', 5)

if __name__ == "__main__":
  main()
