import logging
import os
import re

import pandas as pd 

from src.utils import load_data_file, write_to_json

logging.basicConfig(filename='../log/log.log', encoding='utf-8', level=logging.DEBUG)
log = logging.getLogger(__name__)


def preprocess_data(df, file_type): 
    """
    This function does some minor preprocess cleaning for the data. 
    It could be enriched significantly and usually constitute an entire shared submodule 
    to avoid boilerplate code between projects / use cases . 
    :param df: dataframe to clean
    :param file_type: parameter defining the actions to do 
    :return: clean dataframe
    """
    if file_type == "pubmed":
        df['title'] = df['title'].str.lower()
        log.info(f"the dataframe has been successfully cleaned")
    elif file_type == "clinical_trials":
        df["scientific_title"] = df['scientific_title'].str.lower()
        log.info(f"the dataframe has been successfully cleaned")
    else:
        log.error(f"unknown file type {file_type}, known types are ['pubmed', 'clinical_trials']")
    return df


def load_all_data(root_folder, name):
    """
    This high level function will load and clean all the right data files at once and return a single dataframe.
    It can be changed to be adapted to the architecture of the datalake. 
    :param root_folder: path to the datalake
    :param name: name of directory containing the wanted input data
    :return: Pd.Dataframe 
    """
    log.info(f"loading all {name} data")
    log.info([f"files found in {name} : {files}" for path, _, files in os.walk(root_folder + name)])
    log.info(f"the loader will only load files starting by {name}")
    df = pd.concat([preprocess_data(load_data_file(os.path.join(path, file)), name) for path, _, files in
               os.walk(root_folder + name) for file in files if file.startswith(name)], ignore_index=True)
    df.name = name
    return df


def compute_data_frame(df_data, df_drug, column_name):
    """
    This function does the actual work of transforming the data to answer the problematic.
    The 'title' name can be different between pubmed and clinical_trials, so I chose to implement a parameter 
    column_name to have a single function and avoid boilerplate code
    :param df_data: dataframe containing pubmed or clinical_trials data
    :param df_drug: dataframe containing drugs data
    :param column_name: name of the changing column between clinical_trials and pubmed
    :return: computed dataframe
    """
    log.debug(f"merging {df_drug.name} and {df_data.name}")
    pat = "|".join([re.escape(x.lower()) for x in df_drug.drug])
    df_data.insert(0, 'drug', df_data[column_name].str.extract("(" + pat + ')', expand=False))
    
    log.debug("denormalize the dataframe for output formatting")
    df_fin = df_data.merge(pd.DataFrame({"type": ["journal", column_name]}), how='cross')
    df_fin.loc[df_fin['type'] == 'journal', ['value']] = df_fin['journal']
    df_fin.loc[df_fin['type'] == column_name, ['value']] = df_fin[column_name]
    df_fin = df_fin[["drug", "type", "date", "value"]]
    df_fin.name = df_data.name
    log.info(f"dataframe computed with success")
    return df_fin


def main_compute(root_folder, output_path):
    """
    This function encapsulate all the ETL pipeline. 
    It's mostly hardcoded but at some point too much parametrisation will only cost more time.
    It will load all the data, do the computing work and write everything in a single json file. 
    :param root_folder: path to datalake
    :param output_path: path to output file
    :return: None
    """
    # load data into pandas dataframes 
    df_drugs = load_all_data(root_folder,"drugs")  
    df_pubmed = load_all_data(root_folder, "pubmed")
    df_trials = load_all_data(root_folder, "clinical_trials")

    # compute the work and concat into a single dataframe 
    df_out = pd.concat([compute_data_frame(df_pubmed, df_drugs, "title"),
                        compute_data_frame(df_trials, df_drugs, "scientific_title")])
    # df_out['value'] = df_out['value'].str.encode('raw-unicode-escape', 'ignore').str.decode('raw-unicode-escape')

    # write the output into a single json file
    write_to_json(df_out, output_path)


if __name__ == "__main__":
    main_compute("../data/", "../output/test.json")
