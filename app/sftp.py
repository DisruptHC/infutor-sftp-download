import os
import logging
import typing

import subprocess

import paramiko

from stat import S_ISDIR

class SFTP:
    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
    def _list_files_r(self, sftp, root):
        files = []
        for path in sftp.listdir_attr(root):
            if path.filename.startswith("."):
                continue
            if S_ISDIR(path.st_mode):
                for filename in self._list_files_r(sftp, os.path.join(root, path.filename)):
                    files.append(path.filename + "/" + filename)
            else:
                files.append(path.filename)
        return files
    def list_files(self):
        transport = paramiko.Transport((self.hostname, self.port))
        transport.set_keepalive(30)
        transport.connect(None, self.username, self.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return self._list_files_r(sftp, ".")
    def get(self, src, dst):
        logging.info(f"Downloading './{src}' to '{dst}'...")
        sftp = subprocess.run(
            ["sshpass", "-p", f"{self.password}", "sftp", f"{self.username}@{self.hostname}:{src}", f"{dst}"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        if sftp.returncode:
            logging.info(f"Error: Could not download './{src}' to '{dst}'.")