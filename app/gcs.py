import os
import logging
import glob

from google.cloud import storage

from app.utils import remove_prefix

storage.blob._DEFAULT_CHUNKSIZE = 32 * 1024* 1024  # 32 MB
storage.blob._MAX_MULTIPART_SIZE = 32 * 1024* 1024  # 32 MB

class GCS:
    def __init__(self, project, bucket):
        self.project = project
        self.bucket = bucket
        self.client = None
    def exists(self, dst):
        if not self.client:
            self.client = storage.Client(project=self.project)
        bucket = self.client.bucket(self.bucket)
        blob = bucket.blob(dst)
        return blob.exists()
    def list_files(self, prefix=None):
        if not self.client:
            self.client = storage.Client(project=self.project)
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter=None)
        get_relative_path = lambda blob: remove_prefix(blob.name, prefix) if prefix else blob.name
        return list(map(get_relative_path, blobs))
    def _get_file(self, src, dst):
        if not self.client:
            self.client = storage.Client(project=self.project)
        bucket = self.client.bucket(self.bucket)
        blob = bucket.blob(src)
        logging.info(f"Downloading 'gs://{self.bucket}/{src}' to '{dst}'...")
        blob.download_to_filename(dst)
    def _get_dir(self, src, dst):
        for filename in self.list_files(prefix=src):
            self._get_file(src+filename, dst+filename)
    def get(self, src, dst):
        if not self.exists(src):
            return self._get_dir(src, dst)
        return self._get_file(src, dst)
    def _put_file(self, src, dst):
        if not self.client:
            self.client = storage.Client(project=self.project)
        bucket = self.client.bucket(self.bucket)
        blob = bucket.blob(dst)
        logging.info(f"Uploading '{src}' to 'gs://{self.bucket}/{dst}'...")
        blob.upload_from_filename(src)
    def _put_dir(self, src, dst):
        for filename in glob.glob(src + '/**'):
            if not os.path.isfile(filename):
                self._put_dir(filename + "/", dst + os.path.basename(filename) + "/")
            else:
                self._put_file(filename, dst + os.path.basename(filename))
    def put(self, src, dst):
        if os.path.isfile(src):
            self._put_file(src, dst)
        if os.path.isdir(src):
            self._put_dir(src, dst)
