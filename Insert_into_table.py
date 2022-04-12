import psycopg2
import time

#number of Rows
n = 10000

# Generate single INSERT query
single_query = """INSERT into post (user_id, post_text)
VALUES(1, 'All work and no play makes jack a dull boy.');"""

#Generate Large query
Large_query = "INSERT into post (user_id, post_text) VALUES "

for i in range(n):
	Large_query += "(1, 'All work and no play makes jack a dull boy.'),"

Large_query = Large_query.strip(',') + ';' # Replace Trailing  ',' With ';'

#Connect to database & create cursor
password = open('password.txt','r').read()
conn = psycopg2.connect ("dbname=gajraj, user=postgres password={0}".format(password))
cur = conn.cursor()

#Time the 'n' individual queries
start_time = time.time()
for i in range(n):
	cur.execute(single_query)
conn.commit()
stop_time = time.time()

print("{0} first query took {1} Seconds".format(n, stop_time - start_time))

#execute the Large_query
start_time = time.time()
cur.execute(Large_query)
conn.commit()
stop_time = time.time()

print("The query with {0} rows took {1} Seconds".format(n, stop_time - start_time))

#close the cursor and conection to the database

cur.close()
conn.close()