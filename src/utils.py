import json
import logging 
import os
import shutil

from jsoncomment import JsonComment
import pandas as pd

log = logging.getLogger(__name__)


def copy_data_to_temp(file_name, data_fold, temp_fold):
    os.makedirs(temp_fold, exist_ok=True)
    shutil.copy(data_fold + file_name, temp_fold + file_name)
    return temp_fold + file_name


def load_data_file(file_path):
    """
    This function will try to load the input file into a pd dataframe using the extension of the file
    to call the corresponding pd loader.
    :param file_path: path to datafile
    :return: dataframe
    """
    log.info(f"loading file into a pd dataframe : {file_path}")
    if file_path.endswith(".csv"):
        raw_df = pd.read_csv(file_path, sep=",", header=0)
        return raw_df
    elif file_path.endswith(".json"):
        try:
            raw_df = pd.read_json(file_path, orient="records", dtype=False)
            return raw_df
        except ValueError as e:
            log.warning(f"ValueError while loading json file : {e}")
            log.warning("Trying to load the broken json with a workaround")
            with open(file_path) as data_file:
                parser = JsonComment(json)
                data = parser.load(data_file)
            workaround_df = pd.DataFrame(data)
            log.info("Workaround successfully load the file")
            return workaround_df

    else:
        log.warning("File type not supported yet, supported extensions are : ['csv', 'json']")


def load_processed_file(file_path):
    log.info(f"loading file into a pd dataframe : {file_path}")
    raw_df = pd.read_json(file_path, orient="table")
    return raw_df 


def write_to_json(df_out, path):
    """
    Write a dataframe into a single json file in the expected format
    :param df_out: dataframe
    :param path: path to output
    :return: None 
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df_out.to_json(path, orient='table', index=False)
    log.info(f"Data successfully written in {path}")
