from google.cloud import secretmanager

def get_secret(project, secret, version):
    name = f"projects/{project}/secrets/{secret}/versions/{version}"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')
