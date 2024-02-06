import pandas as pd 
import datetime 

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    print("Passenger with count 0 before preprocessing: ", data['passenger_count'].isin([0]).sum())
    print("Passengers with trip distance 0 before processing: ", data['trip_distance'].isin([0]).sum())
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.lpep_pickup_datetime = data.lpep_pickup_datetime.apply(lambda d: datetime.datetime.fromtimestamp(d.timestamp()).strftime('%Y-%m-%d %H:%M:%S'))
    data.lpep_dropoff_datetime = data.lpep_dropoff_datetime.apply(lambda d: datetime.datetime.fromtimestamp(d.timestamp()).strftime('%Y-%m-%d %H:%M:%S'))

    # convert headers to snake_case format
    data.columns = (
        data.columns
        .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
        .str.lower()
    )

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_snakecase(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns
