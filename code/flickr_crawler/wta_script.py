import MySQLdb as sqldb
from WTA_Hasher import WTAHasher

server = 'cayley.cc.gt.atl.ga.us'
user = 'external_fl_user'
pswd = '4U6tQqBnJUX6Gazu'
db = 'flickr_db'

db_connection = sqldb.connect(server, user, pswd, db)
cursor = db_connection.cursor()

query = 'SELECT * FROM Image_Histogram'
cursor.execute(query)
results = cursor.fetchall()
img_id = []
l_results = []
a_results = []
b_results = []
for result in results:
    l_res = list(result)[1:17]
    a_res = list(result)[17:33]
    b_res = list(result)[33:]
    l_res.append(result[0])
    a_res.append(result[0])
    b_res.append(result[0])
    l_results.append(l_res)
    a_results.append(a_res)
    b_results.append(b_res)

# Hash the results.
l_hasher = WTAHasher(8, 8, l_results)
print "L hashes"
print l_hasher._permutations
new_l_hashes = l_hasher.hashDataset()

# Hash the results.
a_hasher = WTAHasher(8, 8, a_results)
print "A hashes"
print a_hasher._permutations
new_a_hashes = a_hasher.hashDataset()

# Hash the results.
b_hasher = WTAHasher(8, 8, b_results)
print "B hashes"
print b_hasher._permutations
new_b_hashes = b_hasher.hashDataset()

for idx in range(len(new_l_hashes)):
    insert_query = "INSERT INTO `Image_Histogram_WTA`" + \
        "(`image_id`,`l1`,`l2`,`l3`,`l4`,`l5`,`l6`,`l7`,`l8`,`a1`,`a2`,`a3`,`a4`,`a5`,`a6`,`a7`,`a8`,`b1`,`b2`,`b3`,`b4`,`b5`,`b6`,`b7`,`b8`)" + \
        " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24})".format(
            new_l_hashes[idx][-1],
            new_l_hashes[idx][0],
            new_l_hashes[idx][1],
            new_l_hashes[idx][2],
            new_l_hashes[idx][3],
            new_l_hashes[idx][4],
            new_l_hashes[idx][5],
            new_l_hashes[idx][6],
            new_l_hashes[idx][7],
            new_a_hashes[idx][0],
            new_a_hashes[idx][1],
            new_a_hashes[idx][2],
            new_a_hashes[idx][3],
            new_a_hashes[idx][4],
            new_a_hashes[idx][5],
            new_a_hashes[idx][6],
            new_a_hashes[idx][7],
            new_b_hashes[idx][0],
            new_b_hashes[idx][1],
            new_b_hashes[idx][2],
            new_b_hashes[idx][3],
            new_b_hashes[idx][4],
            new_b_hashes[idx][5],
            new_b_hashes[idx][6],
            new_b_hashes[idx][7]
            )
    cursor.execute(insert_query)

db_connection.commit()