# from snakecase import snakecase

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
    print("Pssengers with trip distance 0 before processing: ", data['trip_distance'].isin([0]).sum())
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data['lpep_pickup_date_str'] = data['lpep_pickup_datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data.columns = (
        data.columns
        .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
        .str.lower()
    )

    print(data['vendor_id'].unique())

    return data[(data['passenger_count']>0) & (data['trip_distance']>0)]

@test
def test_output_snakecase(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns   

@test
def test_output_passenger_count(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['passenger_count']>0).all()    


@test
def test_output_trip_dist(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['trip_distance']>0).all()