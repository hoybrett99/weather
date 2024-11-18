import logging
import azure.functions as func
import requests
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */30 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def WeatherCollector(myTimer: func.TimerRequest) -> None:
    try:
        if myTimer.past_due:
            logging.warning('The timer is past due!')

        logging.info('Python timer trigger function started.')

        # API key and URL
        API_KEY = ""
        BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

        # List of cities
        cities = ["London", "New York", "Tokyo", "Paris", "Sydney", "Manila", "Jakarta", "Berlin", "Madrid", 
                  "Seoul", "Bangkok", "Singapore", "Mumbai", "Los Angeles", "Edinburgh", "Dublin", 
                  "Porto", "Cape Town", "Auckland", "Rio de Janeiro", "Lima", "Montevideo", "Buenos Aires"]

        # Initialize an empty list to store data
        weather_data_list = []

        for city in cities:
            try:
                url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
                response = requests.get(url)

                if response.status_code == 200:
                    # Parse JSON response
                    data = response.json()
                    # Normalize JSON data
                    normalized_data = pd.json_normalize(data, sep='_')
                    normalized_data['city'] = city
                    weather_data_list.append(normalized_data)
                else:
                    logging.error(f"Failed to fetch data for {city}: {response.status_code}, {response.reason}")
            except Exception as e:
                logging.error(f"Error while fetching data for {city}: {str(e)}")

        # Combine all city data into a single DataFrame
        if weather_data_list:
            weather_df = pd.concat(weather_data_list, ignore_index=True)
            weather_df["ifUK"] = np.where(weather_df['sys_country'] == "GB", 1, 0)

            # Azure Blob Storage credentials
            STORAGE_ACCOUNT_KEY = ""
            CONTAINER_NAME = "weatherparquet"
            STORAGE_ACCOUNT_NAME = "weatherforecastworld"
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

            # Initialize BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            # Get today's date and time for partitioning
            current_datetime = datetime.now()
            date_partition = current_datetime.strftime("%Y-%m-%d")
            time_partition = current_datetime.strftime("%H-%M-%S")

            # Blob name with folder structure
            blob_name = f"{date_partition}/{time_partition}/weather_data.parquet"

            # Convert DataFrame to Parquet
            parquet_buffer = BytesIO()
            table = pa.Table.from_pandas(weather_df)
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)  # Move buffer pointer to the beginning

            # Upload the Parquet file to Azure Blob Storage
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)
            container_client.upload_blob(name=blob_name, data=parquet_buffer, overwrite=True)

            logging.info(f"Weather data successfully uploaded to Azure Blob Storage at {blob_name}.")
        else:
            logging.warning("No weather data collected from any city.")

    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")
