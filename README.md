# serverless-full-stack-apps-azure-synapse

https://learn.microsoft.com/en-us/azure/developer/python/tutorial-deploy-serverless-cloud-etl-01
https://learn.microsoft.com/en-us/azure/developer/python/configure-local-development-environment

https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-python?pivots=python-mode-decorators
https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-cli-python?pivots=python-mode-decorators&tabs=azure-cli%2Cbash

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

func init LocalFunctionProj --python -m V2

azurite

# to kill : 
sudo lsof -i :{Port number}
kill -9 {PID}

# local.settings.json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "AzureWebJobsStorage": "UseDevelopmentStorage=true"
  }
}

cd LocalFunctionProj
func start