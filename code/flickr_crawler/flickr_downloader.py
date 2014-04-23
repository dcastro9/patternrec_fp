# Copyright 2014, All Rights Reserved
# Author: Daniel Castro <dcastro9@gatech.edu>

import MySQLdb as sqldb
import os
import urllib
import time

class FlickrDownloader(object):
    """ Represents a Flickr Image Downloader
    
    Requires a SQL Database with the following SQL table:
        Photo
            id - int(13) - Incremental id to sort by (unique - primary key).
            flickr_id - int(15) - Image ID provided in the Flickr API.
            farm_id - tinyint(3) - See Flickr API.
            server_id - smallint(5) - See Flickr API.
            secret - varchar(15) - See Flickr API.
    
    Attributes:
        download_directory: Path where images will be stored, as a string.
        image_size: One character string to denote size of image download*.
        sql_server: Server location as a string.
        sql_username: SQL user as a string.
        sql_password: SQL password as a string.
        sql_db: Name of the SQL Database you are accessing.

    *See http://www.flickr.com/services/api/misc.urls.html for details on
    image_size.
    """
    
    TOP_BUCKET_SIZE = 100000
    SUB_BUCKET_SIZE = 10000

    def __init__(self, download_directory, image_size, sql_server,
                 sql_username, sql_password, sql_db):
        """Creates a FlickrDownloader to download images from
        a database of Flickr image entries.
        """
        # TODO(dcastro): There's a raise condition unaccounted for here.
        # (When a directory fails to be created - OSError).
        if not os.path.isdir(download_directory):
                os.makedirs(download_directory)
        self._download_directory = download_directory
        if 'mstzb'.find(image_size)!=-1:
            self._image_size = image_size
        else:
            self._image_size = 's'
        # TODO(dcastro): Catch or throw login errors here.
        self._db_connection = \
            sqldb.connect(sql_server, sql_username, sql_password, sql_db)
        self._db_cursor = self._db_connection.cursor()
        # TODO(dcastro): Check if database & columns exist before querying.
        # Error out gracefully if the db is not there.
        self._db_cursor.execute("SELECT COUNT(*) From Photo WHERE 1;")
        self._num_entries = self._db_cursor.fetchone()[0]
        folder_list = [name for name in os.listdir(self._download_directory)
            if os.path.isdir(os.path.join(self._download_directory, name))]
        # Check download directory for previously saved images.
        if (len(folder_list) > 0):
            folder_list.sort()
            last_folder_path = os.path.join(self._download_directory,
                                            folder_list[len(folder_list)-1])
            sub_folders = [name for name in os.listdir(last_folder_path)
                if os.path.isdir(os.path.join(last_folder_path, name))]
            sub_folders.sort()
            last_sub_folder = os.path.join(last_folder_path, 
                sub_folders[len(sub_folders)-1])
            num_files = len([name for name in os.listdir(last_sub_folder) if 
                os.path.exists(os.path.join(last_sub_folder, name))])
            self._start_value = (len(folder_list)-1)*self.TOP_BUCKET_SIZE + \
                (len(sub_folders)-1)*self.SUB_BUCKET_SIZE + num_files
            self._top_folder_value = len(folder_list)-1
            self._sub_folder_value = len(sub_folders)-1
        else:
            self._start_value = 0
            self._top_folder_value = 0
            self._sub_folder_value = 0

    def download_all(self):
        """Begins downloading all the images.
        """
        while(self._start_value <= self._num_entries):
            if self._start_value%self.TOP_BUCKET_SIZE != 0:
                amount_remaining = self.TOP_BUCKET_SIZE - self._start_value + \
                    self._top_folder_value*self.TOP_BUCKET_SIZE
                #TODO(dcastro): Add logging.
            else:
                amount_remaining = self.TOP_BUCKET_SIZE
            self._db_cursor.execute("SELECT `farm_id`, `server_id`, " + \
                "`flickr_id`,`secret` FROM Photo ORDER BY `id` ASC" + \
                " LIMIT " + str(self._start_value) + "," + \
                str(amount_remaining))
            current_entries = self._db_cursor.fetchall()
            top_level_directory = os.path.join(self._download_directory,
                str(self._top_folder_value))
            if not os.path.isdir(top_level_directory):
                os.makedirs(top_level_directory)
            num_files_in_sf = self._start_value - \
                self._top_folder_value*self.TOP_BUCKET_SIZE - \
                self._sub_folder_value*self.SUB_BUCKET_SIZE
            sub_level_directory = ""
            for entry_count in range(0, len(current_entries)):
                if num_files_in_sf%self.SUB_BUCKET_SIZE == 0:
                    num_files_in_sf = 0
                    sub_level_directory = os.path.join(top_level_directory,
                        str(self._sub_folder_value))
                    if not os.path.isdir(sub_level_directory):
                        os.makedirs(sub_level_directory)
                    self._sub_folder_value += 1
                elif sub_level_directory == "":
                    sub_level_directory = os.path.join(top_level_directory,
                        str(self._sub_folder_value))
                    self._sub_folder_value += 1
                image_url = self.__generateFlickrURL(
                    current_entries[entry_count])
                local_path = os.path.join(sub_level_directory,
                                          image_url.split("/")[-1])
                urllib.urlretrieve(image_url, local_path)
                #TODO(dcastro): Handle error case where its not an image.
                num_files_in_sf += 1
                time.sleep(0.5) # Politeness.
            self._start_value += amount_remaining
            self._sub_folder_value = 0
            self._top_folder_value += 1

    def __generateFlickrURL(self, row):
        """Private method to generate a Flickr URL.
        
        Provides the Flickr URL when given a list that
        represents a row from the `Photo` table in your
        database.
        
        Returns:
           A string URL.
        """
        return "http://farm{0}.staticflickr.com/{1}/{2}_{3}_{4}.jpg".format(
            row[0], row[1], row[2], row[3], self._image_size)

    def close(self):
        """Closes the Flickr SQL Connection.
        """
        self._db_connection.close()


    @property
    def download_directory(self):
        """Path where images will be stored, as a string.
        """
        return self._download_directory
    
    @property
    def num_entries(self):
        """Number of Flickr Photo entries detected.
        """
        return self._num_entries