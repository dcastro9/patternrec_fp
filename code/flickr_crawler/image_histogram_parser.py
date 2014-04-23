# Copyright 2014, All Rights Reserved
# Author: Daniel Castro <dcastro9@gatech.edu>

import MySQLdb as sqldb
from os import listdir, walk, path
from os.path import basename, isfile, join
import cv2
import numpy as np


class ImageHistogramParser(object):
    """ Represents a Image Histogram Parser
    
    Requires a SQL Database with the following SQL table:
        Image_Histogram
            image_id - int(13) - Foreign Key to the image.
            l_channel_bin_1 - First bin for L channel.
            l_channel_bin_2 - Second bin.
            l_channel_bin_n - nth bin.
            a_channel_bin_1 - First bin for A channel.
            a_channel_bin_2 - Second bin.
            a_channel_bin_n - nth bin.
            b_channel_bin_1 - First bin for B channel.
            b_channel_bin_2 - Second bin.
            b_channel_bin_n - nth bin.
    
    Attributes:
        image_directory: Path where images are stored - we assume the
            image has the name of <flickr_id>_<image_secret>_<image_size>.jpg*
        sql_server: Server location as a string.
        sql_username: SQL user as a string.
        sql_password: SQL password as a string.
        sql_db: Name of the SQL Database you are accessing.

    *See http://www.flickr.com/services/api/misc.urls.html for details on
    image_size.  

    (GTWifi)
    """

    SQL_HISTOGRAM_INSERTION = "INSERT IGNORE INTO `flickr_db`.`Image_Histogram` " + \
        "(`image_id`, `l_b_1`, `l_b_2`, `l_b_3`, `l_b_4`, `l_b_5`, `l_b_6`," + \
        " `l_b_7`, `l_b_8`, `l_b_9`, `l_b_10`, `l_b_11`, `l_b_12`, `l_b_13`," + \
        " `l_b_14`, `l_b_15`, `l_b_16`, `a_b_1`, `a_b_2`, `a_b_3`, `a_b_4`," + \
        " `a_b_5`, `a_b_6`, `a_b_7`, `a_b_8`, `a_b_9`, `a_b_10`, `a_b_11`," + \
        "`a_b_12`, `a_b_13`, `a_b_14`, `a_b_15`, `a_b_16`, `b_b_1`, `b_b_2`," + \
        " `b_b_3`, `b_b_4`, `b_b_5`, `b_b_6`, `b_b_7`, `b_b_8`, `b_b_9`," + \
        "`b_b_10`, `b_b_11`, `b_b_12`, `b_b_13`, `b_b_14`, `b_b_15`, `b_b_16`)"

    def __init__(self, image_directory, sql_server, sql_username,
                 sql_password, sql_db):
        """Creates a Image Histogram Parser to create histogram entries
        for the images in a specific folder.
        """
        # TODO(dcastro): Catch or throw login errors here.
        self._db_connection = \
            sqldb.connect(sql_server, sql_username, sql_password, sql_db)
        self._db_cursor = self._db_connection.cursor()
        self._image_directory = image_directory
        # TODO(aneal): Here you should look into the image directory,    
        # and try to create a list of all the image filenames so you
        # can begin to iterate through them, read in the image, and
        # compute their histogram.

        self._image_list = []

        for dirpath, dirnames, filenames in walk(self._image_directory):
            for filename in [f for f in filenames if f.endswith(".jpg")]:
                self._image_list.append(path.join(dirpath, filename))




    def compute_all(self):
        """Computes the histogram for all of the images.
        """
    # TODO Create the bin array and properly store.
    #
        counter = 0
        for image_path in self._image_list:
            # Make sure you catch what happens if it doesn't find the image.
            print basename(image_path).split('_')[0]
            current_image = cv2.imread(image_path)
            current_image = self.__convertToLAB(current_image)
            if current_image != None:
                l_channel,a_channel,b_channel = cv2.split(current_image)
                # 1. Iterate through the channels.
                hist_l = np.histogram(l_channel,16)[0]
                hist_a = np.histogram(a_channel,16)[0]
                hist_b = np.histogram(b_channel,16)[0]

                # 2. Execute a SQL query that stores the bins.
                image_id = basename(image_path).split('_')[0]
                self.__insertHistograms(image_id, hist_l, hist_a, hist_b)

                if(counter%1000==0):
                    print "Computing..."
                    self._db_connection.commit()
                counter+=1



    def __insertHistograms(self, image_id, hist_l, hist_a, hist_b):
        #print image_id
        sql_query = self.SQL_HISTOGRAM_INSERTION + "VALUES ('" + image_id + "'"

        for var in np.append(np.append(hist_l,hist_a),hist_b):
            sql_query += ",'" + str(var) + "'" 
        sql_query += ");"

        self._db_cursor.execute(sql_query)

    def __convertToLAB(self, image):
        """Converts the image to LAB colorspace.
        """
        try:
            return cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
        except cv2.error as e:
            return None
