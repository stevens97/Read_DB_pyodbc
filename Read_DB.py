# Import Libraries
# ------------------------------------

import numpy as np  # for array- and numerical data handling.
import pandas as pd  # for managing data within dataframes.
pd.set_option('mode.chained_assignment', None)  # Allow for chain assignments.
import pyodbc
from tqdm import tqdm

# Connection setup
# ------------------------------------

def SETUP_CONNECTION(DRIVER, SERVER, DATABASE, USERNAME, PASSWORD):
    
    CONNECTION = pyodbc.connect(
        'DRIVER=' + DRIVER + ';SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD + ';Authentication=ActiveDirectoryPASSWORD')

    return CONNECTION

# Close connection
# ------------------------------------

def CLOSE_CONNECTION(CONNECTION):

    CONNECTION.close()

    return None

if __name__ == "__main__":
    
    # First set up connection
    # ------------------------------------
    
    CONNECTION = SETUP_CONNECTION(DRIVER, SERVER, DATABASE, USERNAME, PASSWORD)
    
    # Count rows in table
    # ------------------------------------
    
    QUERY_COUNT = """
        SELECT COUNT(*) FROM TABLE
        """
    
    total_rows =  pd.read_sql_query(QUERY_COUNT, CONNECTION).values[0, 0]
    
    # Read table from database (in chunks) with loading bar
    # ------------------------------------------------------
    
    # Read table
    QUERY = """
        SELECT * FROM TABLE
    """
    
    # Chunk size should be set to desired value
    chunks = pd.read_sql_query(QUERY, CONNECTION, chunksize=1000)
    # Concatenate results to final dataframe
    DF = tqdm(chunks, total=total_rows/rows_in_chunk)
    DF = pd.concat(DF)
    
    # Close connection
    # ------------------------------------------------------
    CLOSE_CONNECTION(CONNECTION)
