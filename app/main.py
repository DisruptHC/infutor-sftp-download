import click

from app.infutor import run

# DRY_RUN = True

# project_id = "dtl-unt-genaiz-app-test"

# sftp_hostname = "104.196.115.148"
# sftp_username = "chakshu"
# sftp_password = "chakshu"

# bucket_name = "dtl-si-infutor-bucket-python"

# pgp_key_secret = "infutor-pgp-key"
# pgp_passphrase_secret = "infutor-pgp-passphrase"

class Options:
    def __init__(
        self, project_id, sftp_hostname, sftp_port, sftp_username, sftp_password_secret, 
        bucket_name, pgp_key_secret, pgp_passphrase_secret, dry_run
    ):
        self.project_id = project_id
        self.sftp_hostname = sftp_hostname
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password_secret = sftp_password_secret
        self.sftp_root = sftp_root
        self.bucket_name = bucket_name
        self.pgp_key_secret = pgp_key_secret
        self.pgp_passphrase_secret = pgp_passphrase_secret
        self.dry_run = dry_run

@click.command()
@click.option('--project-id', 'project_id', required=True, type=str)
@click.option('--hostname', 'sftp_hostname', required=True, type=str)
@click.option('--port', 'sftp_port', required=True, type=int, default=22)
@click.option('--username', 'sftp_username', required=True, type=str)
@click.option('--password-secret', 'sftp_password_secret', required=True, type=str)
@click.option('--sftp_root', 'sftp_root', default="", required=False, type=str)
@click.option('--bucket', 'bucket_name', required=True, type=str)
@click.option('--pgp-key-secret', 'pgp_key_secret', required=True, type=str)
@click.option('--pgp-passphrase-secret', 'pgp_passphrase_secret', required=True, type=str)
@click.option('--dry-run', 'dry_run', default=False, required=False, type=bool, is_flag=True)
@click.pass_context
def main(
    ctx, project_id, sftp_hostname, sftp_port, sftp_username, sftp_password_secret, 
    bucket_name, pgp_key_secret, pgp_passphrase_secret, dry_run
):
    opts = Options(
        project_id, 
        sftp_hostname, 
        sftp_port,
        sftp_username, 
        sftp_password_secret,
        sftp_root,
        bucket_name, 
        pgp_key_secret, 
        pgp_passphrase_secret, 
        dry_run
    )
    ctx.obj = opts
    run(opts)

if __name__ == "__main__":
    main()