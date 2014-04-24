import MySQLdb as sqldb

server = 'cayley.cc.gt.atl.ga.us'
user = 'external_fl_user'
pswd = '4U6tQqBnJUX6Gazu'
db = 'flickr_db'

db_connection = sqldb.connect(server, user, pswd, db)
cursor = db_connection.cursor()

query = 'SELECT COUNT(*) as Num_Images ,Tag.name FROM Photo_Tag_Mapper RIGHT JOIN Tag ON tag_id = id GROUP BY tag_id ORDER BY Num_Images DESC LIMIT 40'
cursor.execute(query)
results = cursor.fetchall()

for result in results:
    print result