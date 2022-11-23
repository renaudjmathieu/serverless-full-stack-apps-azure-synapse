import azure.functions as func
import logging
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()

@app.function_name(name="HttpTrigger1")
@app.route(route="hello") # HTTP Trigger
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    abs_acct_name='blobstoragereno'
    abs_acct_url=f'https://{abs_acct_name}.blob.core.windows.net/'
    abs_container_name='demo-cloudetl-data'

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

    except Exception as e:
        logging.info(e)
        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
            )

    return func.HttpResponse("This HTTP triggered function executed successfully.")

@app.function_name(name="HttpTrigger2")
@app.route(route="hello2") # HTTP Trigger
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("HttpTrigger2 function processed a request!!!")