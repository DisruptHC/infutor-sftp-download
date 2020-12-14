#!/usr/bin/env python3
import time
import os

import pysftp
from google.cloud import storage
from google.cloud import pubsub_v1

def diff(list1, list2):
    return list(set(list1) - set(list2))

def filter_zip(filename):
    return filename.endswith("zip")

def remove_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    return string

class SFTP:
    def __init__(self, hostname, username, password):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.filenames = []
    def _empty(self, pathname):
        pass
    def _for_each_file(self, pathname):
        pathname = remove_prefix(pathname, "./")
        if not pathname.startswith("."):
            self.filenames.append(pathname)
    def for_each_file(self):
        with pysftp.Connection(self.hostname, username=self.username, password=self.password) as sftp:
            sftp.walktree(
                remotepath=".", 
                fcallback=self._for_each_file, 
                dcallback=self._empty, 
                ucallback=self._empty, 
                recurse=True
            )
    def get(self, remote, local):
        with pysftp.Connection(self.hostname, username=self.username, password=self.password) as sftp:
            sftp.get(remote, local)

class GCS:
    def __init__(self, project, bucket):
        self.project = project
        self.bucket = bucket
    def get_all_blobs(self, prefix=None):
        storage_client = storage.Client(project=self.project)
        return storage_client.list_blobs(self.bucket, prefix=prefix, delimiter=None)
    def list_all_files(self, prefix=None):
        get_path = lambda blob: remove_prefix(blob.name, prefix) if prefix else blob.name
        blobs = self.get_all_blobs()
        return map(get_path, blobs)

class Publisher:
    def __init__(self, project, topic):
        self.project = project
        self.topic = topic
        self.publisher = pubsub_v1.PublisherClient()
        self.futures = dict()
        self.topic_path = self.publisher.topic_path(self.project, self.topic)
    def cb(self, future, data):
        def callback(future):
            self.futures.pop(data)
        return callback
    def push(self, data):
        future = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        self.futures[data] = future
        future.add_done_callback(self.cb(future, data))

class Subscriber:
    def __init__(self, project, topic, subscription):
        self.project = project
        self.subscription = subscription
        self.subscriber = pubsub_v1.SubscriberClient()
        self.futures = dict()
        self.subscription_path = self.subscriber.subscription_path(self.project, self.subscription)
    def cb(self, message):
        message.ack()
        # time.sleep(45)
    def get(self):
        future = self.subscriber.subscribe(self.subscription_path, self.cb)
        with self.subscriber:
            future.result()        

def process_zip_file():
    pass

def process_gpg_file():
    pass

def process_item(pathname, sftp, gcs):
    filename = os.path.basename(pathname)
    sftp.get(filename, f'/tmp/infutor/raw/12-2020/{filename}')

def main():
    sftp = SFTP(hostname="10.223.193.9", username="chakshutandon", password="chakshutandon")
    sftp.for_each_file()
        
    gcs = GCS(project="dtl-unt-genaiz-app-test", bucket="dtl-si-infutor-bucket-python")
    gcs_raw_files = gcs.list_all_files(prefix="infutor/raw/12-2020/")

    # publisher = Publisher(project="dtl-unt-genaiz-app-test", topic="infutor-download")
    # for item in diff(sftp.filenames, gcs_raw_files):
    #     publisher.push(item)

    job_queue = diff(sftp.filenames, gcs_raw_files)
    
    for item in job_queue:
        process_item(item, sftp, gcs)

    # while publisher.futures:
    #     time.sleep(5)

    # subscriber = Subscriber(project="dtl-unt-genaiz-app-test", topic="infutor-download", subscription="infutor-download")
    # subscriber.get()

if __name__ == "__main__":
    main()