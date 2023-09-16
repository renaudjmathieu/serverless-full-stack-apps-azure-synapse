# serverless-full-stack-apps-azure-synapse

https://github.com/Azure-Samples/msdocs-python-etl-serverless/tree/main
https://azure.github.io/Cloud-Native/Fall-For-IA/HackTogether/
https://www.youtube.com/watch?v=wToHU8Hts9c
https://learn.microsoft.com/en-us/azure/app-service/quickstart-python

python3 -m venv .venv
virtualenv --python="/usr/bin/python3" .venv
.venv/bin/Activate.ps1

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





