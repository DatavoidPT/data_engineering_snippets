import datetime
import json
import urllib.parse
import boto3
from src.lib.logs.logger import Logger
from setup import configs


class ConnectorS3:
    def __init__(self, bucket, endpoint_url=configs.get('S3_ENDPOINT', 'https://s3.amazonaws.com')):

        self.bucket = bucket
        self.bucket_base = 's3a://{}/'.format(self.bucket)
        params = {'endpoint_url': endpoint_url}
        if 'local' in endpoint_url:
            params['aws_access_key_id'] = 'foo'
            params['aws_secret_access_key'] = 'bar'
        try:
            # session = boto3.Session(profile_name='EUBI.Users')
            self.client = boto3.client('s3', **params)

            # self.client = boto3.client('s3', **params)
            self.resource = boto3.resource('s3', **params)
        except ValueError:
            # Use defaults for invalid endpoints
            self.client = boto3.client('s3')
            self.resource = boto3.resource('s3')

    def get_object(self, key):
        """
        Get content of a file on S3 without using Spark
        :param key: String: The S3 key of the file
        :return: String: A string holding the file contents, None if key not found
        """
        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)['Body'].read().decode('utf-8')
        except:
            return None

    def put_object(self, object, key, metadata=None):
        """
        Put data as a file on S3
        :param object: String: A String containing the data to write
        :param key: String: The key under which to put the data
        :param metadata: String: metadata to pass to the object
        """
        if not metadata:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=object, ACL='bucket-owner-full-control')
        else:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=object, Metadata=metadata, ACL='bucket-owner-full-control')

    def upload_file(self, file_name, bucket, destination_name):
        """
                Upload file to S3
                :param file_name: String:
                :param bucket: String:
                :param destination_name: String:
                """
        try:
            not self.client.upload_file(file_name, bucket, destination_name)
        except Exception as ex:
            raise ex

    def list_objects(self, prefix, limit=None, give_size=False, suffix=''):
        """
        List the keys of all objects in a given layer
        :param prefix: String: The name of the layer
        :param limit: Integer: limit the number of files returned by the list objects operation, if None or 0: no limit
        :param give_size: Boolean: Also return the size of the files listed in bytes
        :param suffix:
        :return: List of Strings or (List of Strings, int): A list of keys and if give_size=True, also an integer in bytes
        """
        objects = []
        total_size = 0
        if not limit or limit > 1000:
            paginator = self.client.get_paginator("list_objects")
            page_iterator = paginator.paginate(Bucket=self.bucket, EncodingType='url', Prefix=prefix)
            for page in page_iterator:
                if "Contents" in page:
                    for key in page["Contents"]:
                        if suffix != '':
                            if key["Key"].endswith(suffix):
                                objects.append(urllib.parse.unquote(key["Key"]))
                                total_size += key['Size']
                        else:
                            objects.append(key["Key"])
                            total_size += key['Size']
        else:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if "Contents" in response:
                for key in response['Contents']:
                    if suffix != '':
                        if key["Key"].endswith(suffix):
                            objects.append(key["Key"])
                            total_size += key['Size']
                    else:
                        objects.append(key["Key"])
                        total_size += key['Size']
        if give_size:
            return list(map(lambda key: key.replace('%3D', '='), objects)), total_size
        else:
            return list(map(lambda key: key.replace('%3D', '='), objects))

    def list_objects_with_timestamp(self, prefix):
        objects = []
        paginator = self.client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=self.bucket, EncodingType='url', Prefix=prefix)
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    print("Key:{}".format(urllib.parse.unquote(key["Key"])))
                    objects.append({'Key': urllib.parse.unquote(key["Key"]), "Timestamp": key['LastModified'].replace(tzinfo=None)})
        return objects

    def delete_object(self, key):
        """
        Delete the object with the given key from S3
        :param key: String: The key of the object to delete
        """
        Logger.info("Deleting files:{}".format(key))
        if type(key) == str:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        else:
            delete_dict = {'Objects': list(map(lambda k: {'Key': k}, key))}
            self.client.delete_objects(Bucket=self.bucket, Delete=delete_dict)

    def get_object_with_timestamp(self, key):
        """
        Get content + upload timestamp of a file on S3 without using Spark
        :param key: String: The S3 key of the file
        :return: String, Timestamp: A string holding the file contents and a timestamp holding the timestamp of the S3 upload
        """
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response['Body'].read().decode('latin-1').encode('ascii', 'ignore').decode('utf-8'), response[
            'LastModified']

    def get_object_with_metadata(self, key):
        """
        Get content of a file on S3
        :param key: String: The S3 key of the file
        :return: String: A string holding the file contents, None if not exists
        """
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            return obj['Body'].read().decode('latin-1').encode('ascii', 'ignore').decode('utf-8'), obj.get('Metadata',
                                                                                                           {})
        except:
            return None, None

    def get_subfolders(self, prefix):
        """
        Get a list of all subfolders in S3 folder
        :param prefix: String: the folder key
        :return: List of Strings: Subfolders
        """
        result = self.client.list_objects(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        prefixes = []
        for o in result.get('CommonPrefixes', []):
            prefixes.append(o.get('Prefix'))
        return prefixes

    def create_directory_if_not_exists(self, location):
        """
        Create a directory at the given location
        :param location: String: an S3 location
        """
        location = location.replace(self.bucket, '')
        if self.get_object(location) is None and location.endswith('/'):
            self.put_object('', location)

    def trim_s3_bucket_from_path(self, fullpath):
        """
        Trim the s3 bucket name and only return relative path
        :param fullpath: String: an S3 location:  ex:fullpath=s3a://eda0x7b2263-sbx-eu-west-1/prod/integrated/objects/move/wave/
        :return: String: S3 key: ex return value: /prod/integrated/objects/move/wave/
        """
        return fullpath.replace(self.bucket_base, '')

    # ------------------------------------------------------------------------------------------#
    # Check a key (Folder/File) exists in s3.
    # Param -
    #   1. key = folder or specific file name / path
    # Return -
    #   1. True (if key exists) or False
    # ------------------------------------------------------------------------------------------#

    def check_key_exists(self, key):

        _bucket = self.resource.Bucket(self.bucket)

        objs = list(_bucket.objects.filter(Prefix=key))
        if len(objs) > 0:
            Logger.info(message="Key: {} Exists in S3!".format(key))
            return True
        else:
            Logger.info(message="Key: {} Doesn't exist in S3!".format(key))
            return False

    def get_old_keys(self, key, num_days):

        kwargs = {'Bucket': self.bucket, 'Key': key}

        obj = self.client.head_object(**kwargs)
        files = []
        if str(obj["LastModified"]) < str(datetime.datetime.now() - datetime.timedelta(days=num_days)):
            files.append(key)

        return files

    def delete_objects_in_s3(self, key):

        _bucket = self.resource.Bucket(self.bucket)

        objs = _bucket.objects.filter(Prefix=key)
        objs.delete()

    def copy_data_from_source_to_destination(self, source_key, destination_key, delete_source=False):
        for file in self.list_objects(prefix=source_key):
            self.client.copy_object(CopySource={'Bucket': self.bucket,
                                                'Key': file},
                                    Bucket=self.bucket,
                                    Key=destination_key + '/' + str(file)[str(file).rfind('date_partition'):])
        if delete_source:
            self.delete_objects_in_s3(source_key)

    def list_folders_with_files_count_size(self, folder_to_crawl, file_extension):
        # ------------------------------------------------------------------------------------------#
        #  Get sub-folders with size , number of files , list of keys which has more then one file.
        # Param -
        #   1. folder_to_crawl = Folder to scan all sub-folders and identify the sub-folder which has more then 1 file.
        #   2. file_extension = file extension to search only
        # Return:
        #   1. destination_bucket = Sub folder path as key and value as True / False True Means it has multiple files and False means 1 file.
        #   2. folders_with_multiple_files = Return folders list having multiple files.
        #   3. folder_path_with_number_of_files = Collection containing folder (Key) and number of files (Values)
        #   4. folder_with_size = Collection containing folder (Key) and Size of folder (Values)
        #   5. folder_path_with_files_array = Collection containing folder (Key) and List of files (Values)
        # ------------------------------------------------------------------------------------------#

        folder_key_with_multiple_files_flag = {}
        folders_with_multiple_files = []
        folder_path_with_number_of_files = {}
        folder_path_with_files_array = {}
        folder_with_size = {}
        kwargs = {'Bucket': self.bucket}
        kwargs['Prefix'] = folder_to_crawl
        counter = 0
        while True:
            resp = self.client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(file_extension):
                    counter += 1
                    file_size = obj['Size']
                    file_name = key[(key.rfind('/') + 1):]
                    key = key[0:key.rfind('/')]
                    if key in folder_key_with_multiple_files_flag:
                        folder_key_with_multiple_files_flag[key] = True
                        if key not in folders_with_multiple_files:
                            folders_with_multiple_files.append(key)
                        numer_of_files = folder_path_with_number_of_files[key]
                        numer_of_files += 1
                        folder_path_with_number_of_files[key] = numer_of_files

                        files = folder_path_with_files_array[key]
                        files.append(file_name)
                        folder_path_with_files_array[key] = files

                        sum_file_size = folder_with_size[key]
                        sum_file_size += file_size
                        folder_with_size[key] = sum_file_size
                    else:
                        folder_key_with_multiple_files_flag[key] = False
                        folder_path_with_number_of_files[key] = 1
                        files = []
                        files.append(file_name)
                        folder_path_with_files_array[key] = files
                        folder_with_size[key] = file_size
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        Logger.info("Total Number of Files:{}".format(counter))
        return folder_key_with_multiple_files_flag, folders_with_multiple_files, folder_path_with_number_of_files, folder_with_size, folder_path_with_files_array

    # ------------------------------------------------------------------------------------------#
    # Copy or move files within s3.
    # Param -
    #   1. source = Source s3 location of files.
    #   2. destination = Destination s3 location
    #   3. destination_bucket = destination bucket name. Will use source bucket if kept blank
    #   4. delete_source_after_copy = Flag to move or just copy
    #   5. Suffix = Copy or move only matching files with suffix
    # ------------------------------------------------------------------------------------------#
    def copy_files_inside_folder(self, source, destination, destination_bucket='', delete_source_after_copy=False,
                                 suffix=''):

        if destination_bucket.strip() == '':
            destination_bucket = self.bucket

        kwargs = {'Bucket': self.bucket, 'Prefix': source}

        resp = self.client.list_objects_v2(**kwargs)
        files = []
        for obj in resp['Contents']:
            key = obj['Key']
            if suffix is not '':
                if key.endswith(suffix):
                    files.append(key)
            else:
                files.append(key)

        self.copy_files(destination, files=files, destination_bucket=destination_bucket,
                        delete_source_after_copy=delete_source_after_copy)

    # ------------------------------------------------------------------------------------------#
    # Copy or move file (passed as an argument) within s3.
    # Param -
    #   1. destination = Destination s3 location
    #   2. source_folder = source folder path
    #   3. files = An list of s3 location of file to copy or move.
    #   4. destination_bucket = destination bucket name , if pass as empty then default will be source bucket only.
    #   5. delete_source_after_copy = Flag to move or just copy
    # ------------------------------------------------------------------------------------------#
    def copy_files(self, destination, source_folder='', files=None, destination_bucket='',
                   delete_source_after_copy=False):

        if destination_bucket.strip() == '':
            destination_bucket = self.bucket

        if files:
            for file in files:
                self.client.copy_object(CopySource={'Bucket': self.bucket,
                                                    'Key': (source_folder + "/" if source_folder != '' else '') + file},
                                        Bucket=destination_bucket,
                                        Key=destination + '/' + str(file)[str(file).rfind('/') + 1:])
                if delete_source_after_copy:
                    self.delete_object((source_folder + "/" if source_folder != '' else '') + file)

    def read_json(self, key):
        """
        Read a JSON file from S3 without using spark. This is used for reading the log files
        :param key: String: The S3 key of the file
        :return: Dict: A JSON object containing the file contents
        """
        return json.loads(self.get_object(key))

    def write_json(self, json_obj, key):
        """
        Write a json file to S3
        :param json_obj: Dict: A JSON object
        :param key: String: The key under which to save the file
        """
        self.put_object(json.dumps(json_obj), key)

    def create_folder(self, folder_name_with_path):
        """
        Create a folder
        :param folder_name_with_path:
        :return:
        """
        self.client.put_object(Bucket=self.bucket, Key=folder_name_with_path, ACL='bucket-owner-full-control')

    def delete_folder(self, folder_name_with_path):
        """
        Create a folder
        :param folder_name_with_path:
        :return:
        """
        if self.check_key_exists(folder_name_with_path):
            keys = self.list_objects(folder_name_with_path + "/")
            for key in keys:
                Logger.info("key to delete:{}".format(key))
                self.delete_object(key)

    def download(self, key, filename):
        """
        Download a file from S3 to local disk
        :param key: String: the key on S3
        :param filename: String: The filename on local
        :return: the local filename
        """
        self.resource.Bucket(self.bucket).download_file(key, filename)
        return filename

    def get_object_metadata(self, key):
        """
        Get the metadata from an S3 object
        :return: Dict: metadata from S3
        """
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj.get('Metadata', {})




