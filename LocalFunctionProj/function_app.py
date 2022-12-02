import json
import logging
import os
import pandas as pd
import pyarrow
import fastparquet
from io import StringIO
from datetime import datetime, timedelta

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.storage.filedatalake import DataLakeServiceClient

from shared.azure_credential import (
    get_azure_default_credential,
    get_azure_key_credential,
)
from shared.bing_search import get_news
from shared.blob_storage import upload_to_blob
from shared.hash import get_random_hash
from shared.key_vault_secret import get_key_vault_secret

def return_blob_files(container_client, arg_date, std_date_format):
    start_date = datetime.strptime(arg_date, std_date_format).date() - timedelta(days=1)

    blob_files = [blob for blob in container_client.list_blobs() if blob.creation_time.date() >= start_date]

    return blob_files

def read_csv_to_dataframe(container_client, filename, file_delimiter= ','):
    blob_client = container_client.get_blob_client(blob=filename)

    # Retrieve extract blob file
    blob_download = blob_client.download_blob()

    # Read blob file into DataFrame
    blob_data = StringIO(blob_download.content_as_text())
    df = pd.read_csv(blob_data,delimiter=file_delimiter)
    return df

def write_dataframe_to_datalake(df, datalake_service_client, filesystem_name, dir_name, filename):

    file_path = f'{dir_name}/{filename}'

    file_client = datalake_service_client.get_file_client(filesystem_name, file_path)

    processed_df = df.to_parquet(index=False)

    file_client.upload_data(data=processed_df,overwrite=True, length=len(processed_df))

    file_client.flush_data(len(processed_df))

    return True

def archive_cooltier_blob_file(blob_service_client, storage_account_url, source_container, archive_container, blob_list):

    for blob in blob_list:
        blob_name = blob.name
        source_blob_url = f'{storage_account_url}{source_container}/{blob_name}'

        # Copy source blob file to archive container and change blob access tier to 'Cool'
        archive_blob_client = blob_service_client.get_blob_client(archive_container, blob_name)

        archive_blob_client.start_copy_from_url(source_url=source_blob_url, standard_blob_tier=StandardBlobTier.Cool)

        (blob_service_client.get_blob_client(source_container, blob_name)).delete_blob(delete_snapshots='include')

    return True

def ingest_relational_data(container_client, blob_file_list):
    df = pd.concat([read_csv_to_dataframe(container_client=container_client, filename=blob_name.name) for blob_name in blob_file_list], ignore_index=True)

    return df

def process_relational_data(df, columns, groupby_columns):
    # Remove leading and trailing whitespace in df column names
    processed_df = df.rename(columns=lambda x: x.strip())

    # Clean column names for easy consumption
    processed_df.columns = processed_df.columns.str.strip()
    processed_df.columns = processed_df.columns.str.lower()
    processed_df.columns = processed_df.columns.str.replace(' ', '_')
    processed_df.columns = processed_df.columns.str.replace('(', '')
    processed_df.columns = processed_df.columns.str.replace(')', '')

    # Filter DataFrame (df) columns
    processed_df = processed_df.loc[:, columns]

    # Filter out all empty rows, if they exist.
    processed_df.dropna(inplace=True)

    # Remove leading and trailing whitespace for all string values in df
    df_obj_cols = processed_df.select_dtypes(['object'])
    processed_df[df_obj_cols.columns] = df_obj_cols.apply(lambda x: x.str.strip())

    # Convert column to datetime: attempt to infer date format, return NA where conversion fails.
    processed_df['date'] = pd.to_datetime( processed_df['date'], infer_datetime_format=True, errors='coerce')

    # Convert object/string to numeric and handle special characters for each currency column
    processed_df['gross_sales'] = processed_df['gross_sales'].replace({'\$': '', ',': ''}, regex=True).astype(float)

    # Capture dateparts (year and month) in new DataFrame columns
    processed_df['sale_year'] = pd.DatetimeIndex(processed_df['date']).year
    processed_df['sale_month'] = pd.DatetimeIndex(processed_df['date']).month

    # Get Gross Sales per Segment, Country, Sale Year, and Sale Month
    processed_df = processed_df.sort_values(by=['sale_year', 'sale_month']).groupby(groupby_columns, as_index=False).agg(total_units_sold=('units_sold', sum), total_gross_sales=('gross_sales', sum))

    return processed_df

def load_relational_data(processed_df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    now = datetime.today().strftime("%Y%m%d_%H%M%S")
    processed_filename = f'{file_prefix}_{now}.{file_format}'
    write_dataframe_to_datalake(processed_df, datalake_service_client, filesystem_name, dir_name, processed_filename)
    return True

def run_cloud_etl(service_client, storage_account_url, source_container, archive_container, source_container_client, blob_file_list, columns, groupby_columns, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    df = ingest_relational_data(source_container_client, blob_file_list)
    df = process_relational_data(df, columns, groupby_columns)
    result = load_relational_data(df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix)
    result = archive_cooltier_blob_file(service_client, storage_account_url, source_container, archive_container, blob_file_list)

    return result

app = func.FunctionApp()

@app.function_name(name="CloudETL")
@app.route(route="cloudetl") # HTTP Trigger
def demo_relational_data_cloudetl(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    arg_date = '2014-07-01'
    std_date_format = '%Y-%m-%d'
    processed_file_format = 'parquet'
    processed_file_prefix = 'financial_demo'

    # List of columns relevant for analysis
    cols =  ['segment', 'country', 'units_sold', 'gross_sales', 'date']

    # List of columns to aggregate
    groupby_cols = ['segment', 'country', 'sale_year', 'sale_month']

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        abs_acct_name='blobstoragereno'
        abs_acct_url=f'https://{abs_acct_name}.blob.core.windows.net/'
        abs_container_name='demo-cloudetl-data'
        archive_container_name = 'demo-cloudetl-archive'

        adls_acct_name='datalakereno'
        adls_acct_url = f'https://{adls_acct_name}.dfs.core.windows.net/'
        adls_fsys_name='filesystem'
        adls_dir_name='finance_data'
        adls_secret_name='adls-access-key1'

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True, exclude_visual_studio_code_credential=True)
        
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

        # Initialize Azure Service SDK Clients
        abs_service_client = BlobServiceClient(
            account_url = abs_acct_url,
            credential = az_credential
        )

        abs_container_client = abs_service_client.get_container_client(container=abs_container_name)

        adls_service_client = DataLakeServiceClient(
            account_url = adls_acct_url,
            credential = az_credential
        )

        # Run ETL Application
        process_file_list = return_blob_files(
            container_client = abs_container_client,
            arg_date = arg_date,
            std_date_format = std_date_format
        )

        run_cloud_etl(
            source_container_client = abs_container_client,
            blob_file_list = process_file_list,
            columns = cols,
            groupby_columns = groupby_cols,
            datalake_service_client = adls_service_client,
            filesystem_name = adls_fsys_name,
            dir_name = adls_dir_name,
            file_format = processed_file_format,
            file_prefix = processed_file_prefix,
            service_client = abs_service_client,
            storage_account_url = abs_acct_url,
            source_container = abs_container_name,
            archive_container = archive_container_name
        )

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")

@app.function_name(name="ApiSearch")
@app.route(route="search") # HTTP Trigger
def api_search(req: func.HttpRequest) -> func.HttpResponse:
    # Get the query parameters
    search_term = req.params.get("search_term", "Quantum Computing")
    count = req.params.get("count", 10)

    # Get environment variables
    key_vault_resource_name = os.environ["KEY_VAULT_RESOURCE_NAME"]
    bing_secret_name = os.environ["KEY_VAULT_SECRET_NAME"]
    bing_news_search_url = os.environ["BING_SEARCH_URL"]
    blob_account_name = os.environ.get("BLOB_STORAGE_RESOURCE_NAME")
    blob_container_name = os.environ["BLOB_STORAGE_CONTAINER_NAME"]

    # Get authentication to Key Vault with environment variables
    azure_default_credential = get_azure_default_credential()

    # Get the secret from Key Vault
    bing_key = get_key_vault_secret(
        azure_default_credential, key_vault_resource_name, bing_secret_name
    )

    # Get authentication to Bing Search with Key
    azure_key_credential = get_azure_key_credential(bing_key)

    # Clean up file name
    random_hash = get_random_hash()
    filename = f"search_results_{search_term}_{random_hash}.json".replace(" ", "_").replace(
        "-", "_"
    )

    # Get the search results
    news_search_results = get_news(azure_key_credential, bing_news_search_url, search_term, count)

    # Convert the result to JSON and save it to Azure Blob Storage
    if news_search_results.value:
        news_item_count = len(news_search_results.value)
        logging.info("news item count: %d", news_item_count)
        json_items = json.dumps([news.as_dict() for news in news_search_results.value])

        blob_url = upload_to_blob(
            azure_default_credential,
            blob_account_name,
            blob_container_name,
            filename,
            json_items,
        )
        logging.info("news uploaded: %s", blob_url)

    return filename