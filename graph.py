import click
from google.cloud import bigquery

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
	q1 = """select id, text from `w4111-columbia.graph.tweets` where text like \'%going live%\' and text like \'%https://%\'"""
	job1 = client.query(q1)

	result1 = job1.result()
	return list(result1)
	#return []

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
	q2 = """select SUBSTR(create_time,1,3) AS day, avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by day order by avg_likes desc limit 1"""
	job2 = client.query(q2)

	result2 = job2.result()
	return list(result2)
	#return []

# SQL query for Question 3. You must edit this function.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
	q3 = """select twitter_username as src, substr(REGEXP_EXTRACT(text, "@[a-zA-Z0-9_.+-]+"),2,100) AS dst from `w4111-columbia.graph.tweets` \
			where text like \'%@%\' group by src,dst"""

	job3 = client.query(q3)

	result3 = job3.result()
	

	dataset_id = 'dataset1'

	job_config = bigquery.QueryJobConfig()
	# Set use_legacy_sql to True to use legacy SQL syntax.
	job_config.use_legacy_sql = True
	# Set the destination table
	table_ref = client.dataset(dataset_id).table('edges')
	job_config.destination = table_ref
	job_config.allow_large_results = True

	# Start the query, passing in the extra configuration.
	query_job = client.query(
		q3,
		# Location must match that of the dataset(s) referenced in the query
		# and of the destination table.
		location='US',
		job_config=job_config)  # API request - starts the query

	query_job.result()  # Waits for the query to finish
	print('Query results loaded to table {}'.format(table_ref.path))

	return list(result3)
	#return []

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
	# Query Explanation
	# Table INDEGREE: count the indegree for each user and return the one with the highest indegree, and a column
	# 				  rank, which is 1 (used for join)
	# Table OUTDEGREE: count the outdegree for each user and return the one with the highest outdegree, and a column
	# 				  rank, which is 1 (used for join)
	# Join the two table on the rank so that we have a row that have two columns: max_indegree and max_outdegree

	q = """
	WITH INDEGREE AS(
	SELECT COUNT(DISTINCT src) AS max_indegree, ROW_NUMBER() OVER (ORDER BY max_indegree) AS rank
	FROM GRAPH
	GROUP BY dst
	ORDER BY max_indegree
	LIMIT 1),

	OUTDEGREEE AS(
	SELECT COUNT(DISTINCT dst) AS max_outdegree, ROW_NUMBER() OVER (ORDER BY max_outdegree) AS rank
	FROM GRAPH
	GROUP BY src
	ORDER BY max_outdegree
	LIMIT 1)

	SELECT max_indegree, max_outdegree
	FROM INDEGREE AS I JOIN OUTDEGREE AS O ON rank
	"""

	job4 = client.query(q)

	result = job.result()
	return list(result)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
	# Query Explanation:
	# Table AVGLIKE: get the average like for each user on all their tweets
	# Table INDEGREE: get the indegree of each user
	# Table POPULAR: get the a list of popular people
	# Table UNPOPULAR: get a list of unpopular people
	# Calculate the count of tweet initialized by unpopular people and find out the number of tweets in the previous group
	# that refer a popular people. Then we will get the conditional probability

	q = """
	WITH AVGLIKE AS(
	SELECT twitter_username, AVG(like_num) AS avg
	FROM `w4111-columbia.graph.tweets`
	GROUP BY twitter_username
	),

	INDEGREE AS(
	SELECT dst, COUNT(src) AS count
	FROM GRAPH
	GROUP BY dst
	)

	POPULAR AS(
	SELECT twitter_username
	FROM `w4111-columbia.graph.tweets` 
	WHERE twitter_username IN (SELECT twitter_username FROM AVGLIKE WHERE avg >= (SELECT AVG(avg) FROM AVGLIKE)) AND
	twitter_username IN (SELECT dst FROM INDEGREE WHERE count >= (SELECT AVG(count) FROM INDEGREE))
	)

	UNPOPULAR AS(
	SELECT twitter_username
	FROM `w4111-columbia.graph.tweets` 
	WHERE twitter_username IN (SELECT twitter_username FROM AVGLIKE WHERE avg < (SELECT AVG(avg) FROM AVGLIKE)) AND
	twitter_username IN (SELECT dst FROM INDEGREE WHERE count < (SELECT AVG(count) FROM INDEGREE))
	)

	SELECT (SELECT COUNT(*) FROM SAMPLE WHERE dst IN (SELECT twitter_username FROM POPULAR))::numeric /
	(SELECT COUNT(*) FROM SAMPLE) AS popular_unpopular
	FROM (SELECT COUNT(*) FROM GRAPH WHERE src IN (SELECT twitter_username FROM UNPOPULAR)) AS SAMPLE
	"""

	job = client.query(q)

	result = job.result()
	return list(result)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):

	return []

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):

	return []


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

@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
	client = bigquery.Client.from_service_account_json(pathtocred)

	#funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
	funcs_to_test = [q5]
	#funcs_to_test = [testquery]
	for func in funcs_to_test:
		rows = func(client)
		print ("\n====%s====" % func.__name__)
		print(rows)

	#bfs(client, 'A', 5)

if __name__ == "__main__":
  main()