import MySQLdb as sqldb

server = 'cayley.cc.gt.atl.ga.us'
user = 'external_fl_user'
pswd = '4U6tQqBnJUX6Gazu'
db = 'flickr_db'

db_connection = sqldb.connect(server, user, pswd, db)
cursor = db_connection.cursor()

query = 'SELECT COUNT(Tag.id) as NumImages, Tag.id FROM `Photo_Tag_Mapper` LEFT JOIN Image_Histogram_PID ON photo_id=Image_Histogram_PID.id LEFT JOIN Tag ON tag_id=Tag.id GROUP BY Tag.id ORDER BY NumImages ASC LIMIT 100'
cursor.execute(query)
results = cursor.fetchall()

for result in results:
    print result