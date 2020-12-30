import os
import logging
import datetime
import traceback

from app.sftp import SFTP
from app.gcs import GCS
from app.pgp import PGP

from app.processor import Processor, get_processed_name

from app.secrets import get_secret
from app.utils import rename_with_date, rm

def run(opts):
    # configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s')

    logging.info(f"opts.dry_run: {opts.dry_run}")

    working_dir = "/tmp/infutor"
    today = datetime.date.today().strftime("%m-%Y")

    # import pgp key into keyring
    pgp = PGP(opts.project_id, opts.pgp_key_secret, opts.pgp_passphrase_secret)
    pgp.import_key()

    # list remote SFTP files
    sftp_password = get_secret(opts.project_id, opts.sftp_password_secret, "latest")
    sftp = SFTP(hostname=opts.sftp_hostname, port=opts.sftp_port, username=opts.sftp_username, password=sftp_password)
    remote_files = sftp.list_files()
    logging.info(f"sftp.list_files(): {remote_files}")

    # list raw files in GCS
    gcs = GCS(project=opts.project_id, bucket=opts.bucket_name)
    uploaded_raw_files = gcs.list_files(prefix=f"infutor/raw/{today}/")
    logging.info(f"gcs.list_files(prefix='infutor/raw/{today}/'): {uploaded_raw_files}")

    # configure processor
    processor = Processor(working_dir=working_dir, sftp=sftp, gcs=gcs, pgp=pgp, today=today)

    # get list of unprocessed remote files
    remote_job_queue = []
    for filename in remote_files:
        if rename_with_date(filename, today) not in uploaded_raw_files:
            remote_job_queue.append(filename)
    logging.info(f"remote_job_queue: {remote_job_queue}")

    # process remote files
    count = 0
    if not opts.dry_run:
        for item in remote_job_queue:
            try:
                processor.process_remote_item(item)
                count += 1
                logging.info(f"Processed {count}/{len(remote_job_queue)} remote files.")
            except Exception as err:
                logging.info(traceback.format_exc())
                pass

    # get list of unprocessed and uploaded (orphaned) files
    gcs_job_queue = []
    for filename in uploaded_raw_files:
        processed_name = get_processed_name(filename)
        if processed_name.endswith('/'):
            is_processed = len(gcs.list_files(prefix=f"infutor/processed/{today}/{processed_name}")) > 0
        else:
            is_processed = gcs.exists(dst=f"infutor/processed/{today}/{processed_name}")
        if not is_processed:
            gcs_job_queue.append(filename)
    logging.info(f"gcs_job_queue: {gcs_job_queue}")

    # process orphaned files
    if not opts.dry_run:
        for item in gcs_job_queue:
            try:
                processor.process_gcs_item(item)
            except Exception as err:
                logging.info(traceback.format_exc())
                pass

    # remove working dir
    rm(working_dir)
