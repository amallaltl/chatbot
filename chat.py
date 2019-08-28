import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
sql_transaction = []
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
    (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,parent TEXT,
    comment TEXT, subreddit TEXT, time INT, score INT)""")

def format_data(data):
	data = data.replace("\n"," newlinechar ").replace('"',"'")
	return data

def acceptable(data):
    if len(data.split(' ')) > 50 or len (data)<1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True

def find_parent(pid):
	try:
		sql = "SELECT COMMENT FROM parent_reply where comment_id '{}' LIMIT 1".format(pid)
		c.execute(sql)
		result = c.fetchone()
		if result!= None:
			return result[0]
		else: return false
	except Exception as e:
		print("find_parent", e)
		return false

def sql_insert_replace_comment(comment_id, parent_id, parent_data, comment,subreddit,time, score):
        try:
            sql = """UPDATE parent_reply SET parent_id= ?, comment_id = ?, parent=?, comment=?, subreddit=?, time=?, score=?""".format(parent_id, comment_id, parent_data,comment, subreddit, time,score)
            transaction_bldr(sql)
        except Exception as e:
            print('replace' str(e))

def sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, create_utc, score):
        try:
            sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, time, score) VALUES (?, ?, ?, ?, ?, ?, ?)""".format(parent_id, comment_id, parent_data,comment, subreddit, create_utc, score)
            transaction_bldr(sql)
        except Exception as e:
            print('replace' str(e))

def sql_insert_no_parent(comment_id, parent_id, parent_data, comment,subreddit,time, score):
        try:
            sql = """UPDATE parent_reply SET parent_id= ?, comment_id = ? parent=?, comment=?, subreddit=?, time=?, score=?""".format(parent_id, comment_id, parent_data,comment, subreddit, time,score)
            transaction_bldr(sql)
        except Exception as e:
            print('replace' str(e))

def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT ". format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        print(e)
        return False

if __name__ == "__main__":
    create_table()
    row_counter = 0 
    paired_row = 0
    print("hello")
    with open("E:/Personal/Chatbot/reddit_data/RC_2005-12/RC_2005-12".format(timeframe.split('-')[0],timeframe),buffering=1000) as f:
        for row in f:
            print(row)
            row_counter +=1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            #body = row['body']
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']            
            parent_data = find_parent(parent_id)

            if score>=2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                        if existing_comment_score:
                            if score>existing_comment_score:
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc)
                        else:
                            if parent_data:
                                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, create_utc, score)
                            else:
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                    
                                
