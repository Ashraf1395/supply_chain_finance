import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    
    # URL of the published Google Sheets document
    url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTbL00GcdQp0bJt9wf1ROltMq7s3qyxl-NYF7Pvk79Jfxgwfn9dNWmPD_yJHTDq_Wzvps8EIr6cOKWm/pubhtml'

    # Read data from the Google Sheets document into a DataFrame
    tables = pd.read_html(url,header=0,skiprows=1)

    # Assuming 'leaderboard' is the first table extracted
    leaderboard_df = tables[-1]

    leaderboard_df.drop(columns=['1'],inplace=True)
    # Preview the data


    return leaderboard_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
