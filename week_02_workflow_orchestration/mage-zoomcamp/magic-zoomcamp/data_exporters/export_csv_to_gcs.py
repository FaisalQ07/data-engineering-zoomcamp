import gcsfs
import pyarrow as pa 

import os 

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/ny-rides-faisal-701c4daaae72.json"
bucket = "mage-demo-bucker-zoomcamp-faisal"
project_id = "ny-rides-faisal"
folder = "green_taxi_csv_20240202"
partition_column = "lpep_pickup_date_str"

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here   
    gcs = gcsfs.GCSFileSystem()

    partitions = data[partition_column].unique()
    # print(partitions)
    for partition_value in partitions:
        partitioned_df = data[data[partition_column] == partition_value]
        # print(partitioned_df.head(5))
        filename = f"{folder}/{partition_column}={partition_value}.csv"
        with gcs.open(f"{bucket}/{filename}", "w") as f:
            partitioned_df.to_csv(f, index=False)

    



