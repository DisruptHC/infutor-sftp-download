import os

import base64
from app.secrets import get_secret

from app.utils import mkdir

class PGP:
    def __init__(self, project_id, key_secret, passphrase_secret):
        self.project_id = project_id
        self.key_secret = key_secret
        self.key_file = None
        self.passphrase_secret = passphrase_secret
        self.passphrase_file = None
    def download_key_file(self, version="latest"):
        filename = "/tmp/infutor/infutor.key.bin"
        mkdir(os.path.dirname(filename))
        pgp_key = base64.b64decode(get_secret(self.project_id, self.key_secret, version))
        with open(filename, 'wb') as self.key_file:
            self.key_file.write(pgp_key)
    def download_passphrase_file(self, version="latest"):
        filename = "/tmp/infutor/infutor.passphrase"
        mkdir(os.path.dirname(filename))
        pgp_passphrase = get_secret(self.project_id, self.passphrase_secret, version)
        with open(filename, 'w') as self.passphrase_file:
            self.passphrase_file.write(pgp_passphrase)
    def import_key(self, version="latest"):
        if not self.key_file:
            self.download_key_file()
        if not self.passphrase_file:
            self.download_passphrase_file()
        key_file = os.path.realpath(self.key_file.name)
        cmd = f"gpg --import --batch --yes {key_file}"
        os.system(cmd)
    def decrypt_file(self, src, dst):
        if not self.key_file or not self.passphrase_file:
            self.import_key()
        passphrase_file = os.path.realpath(self.passphrase_file.name)
        mkdir(os.path.dirname(dst))
        cmd = f"gpg --decrypt --output {dst} --pinentry-mode loopback --ignore-mdc-error --passphrase-file {passphrase_file} --batch --yes {src}"
        os.system(cmd)
