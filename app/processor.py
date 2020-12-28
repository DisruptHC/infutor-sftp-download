import os
import zipfile

from app.utils import mkdir, rename_with_date, is_zip_file, is_pgp_file, rm

def get_processed_name(filename):
    if is_zip_file(filename):
        return os.path.splitext(filename)[0] + "/"
    if is_pgp_file(filename):
        return os.path.splitext(filename)[0]
    return filename

class Processor:
    def __init__(self, working_dir, sftp, gcs, pgp, today):
        self.working_dir = working_dir
        self.sftp = sftp
        self.gcs = gcs
        self.pgp = pgp
        self.today = today
    def _process_zip_file(self, item):
        raw_filepath = f"{self.working_dir}/raw/{item}"
        processed_dir = get_processed_name(item)
        processed_dirpath = f"{self.working_dir}/processed/{processed_dir}"
        with zipfile.ZipFile(raw_filepath, 'r') as zip_file:
            zip_file.extractall(processed_dirpath)
        self.gcs.put(processed_dirpath, f"infutor/processed/{self.today}/{processed_dir}")
        rm(processed_dirpath)
    def _process_pgp_file(self, item):
        raw_filepath = f"{self.working_dir}/raw/{item}"
        processed_file = get_processed_name(item)
        processed_filepath = f"{self.working_dir}/processed/{processed_file}"
        self.pgp.decrypt_file(raw_filepath, processed_filepath)
        self.gcs.put(processed_filepath, f"infutor/processed/{self.today}/{processed_file}")
        rm(processed_filepath)
    def _ignore_process_file(self, item):
        raw_filepath = f"{self.working_dir}/raw/{item}"
        processed_file = get_processed_name(item)
        self.gcs.put(raw_filepath, f"infutor/processed/{self.today}/{processed_file}")
    def process_remote_item(self, item):
        filename = rename_with_date(item, self.today)

        raw_filepath = f"{self.working_dir}/raw/{filename}"
        mkdir(os.path.dirname(raw_filepath))

        self.sftp.get(item, raw_filepath)
        self.gcs.put(raw_filepath, f"infutor/raw/{self.today}/{filename}")

        if is_zip_file(filename):
            self._process_zip_file(filename)
        elif is_pgp_file(filename):
            self._process_pgp_file(filename)
        else:
            self._ignore_process_file(filename)

        rm(raw_filepath)
    def process_gcs_item(self, item):
        filename = item
        
        raw_filepath = f"{self.working_dir}/raw/{filename}"
        mkdir(os.path.dirname(raw_filepath))

        self.gcs.get(f"infutor/raw/{self.today}/{filename}", raw_filepath)
        
        if is_zip_file(filename):
            self._process_zip_file(filename)
        elif is_pgp_file(filename):
            self._process_pgp_file(filename)
        else:
            self._ignore_process_file(filename)

        rm(raw_filepath)
        