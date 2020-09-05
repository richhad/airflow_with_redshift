import logging
import boto3
import base64
from botocore.exceptions import ClientError
import json

logger = logging.getLogger(__name__)


def get_secret():

    secret_name = "/databases/redshift/sample"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.warn("Unable to read secrets")
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            secrets = json.loads(secret)
            return {
                "redshift": {
                    "hostname": secrets["host"],
                    "database": secrets["dbname"],
                    "port": secrets["port"],
                    "username": secrets["username"],
                    "password": secrets["password"],
                }
            }
        else:
            base64.b64decode(get_secret_value_response["SecretBinary"])
            logger.warning("Unexpected binary secret")
            return {}