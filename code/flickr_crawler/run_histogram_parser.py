from image_histogram_parser import ImageHistogramParser


ihp = ImageHistogramParser('/home/aneal9/Documents/Research/image_db/',
	'cayley.cc.gt.atl.ga.us', 'external_fl_user', '4U6tQqBnJUX6Gazu', 
	'flickr_db')

ihp.compute_all()
ihp._db_connection.commit()
