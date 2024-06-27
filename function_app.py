import azure.functions as func
import logging
import pickle
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="func_serverless_app")
def func_serverless_app(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    user_id = req.params.get('user_id')
    if not user_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            user_id = req_body.get('user_id')

    connect_str = os.getenv('BLOB_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "samples-workitems"
    blob_name = "data_blob_df.pkl"

    blob_client = blob_service_client.get_blob_client(container=container_name,
                                                      blob=blob_name)
    blob_data = blob_client.download_blob().readall()

    # Charger les données à partir du fichier .pkl
    # nothing change
    df = pickle.loads(blob_data)
    testY = user_id
    testZ = int(testY)
    test = df['article_rec'].iloc[testZ]

    if not user_id:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name \
                in the query string or in the request body for a personalized \
                    response.",
             status_code=200
        )
    else:
        return func.HttpResponse(f"{test}")
