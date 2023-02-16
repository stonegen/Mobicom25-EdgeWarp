#!/usr/bin/env python3
import os
import pandas as pd
from os import path

from utils.context import data_processed_dir
from utils.utils import remove_nan_object
from common import Environment

DEBUG = False

# Edit this variable 
FOLDERNAME = ""

FILENAMES = os.listdir(FOLDERNAME)
for FNAME in FILENAMES:

    if "_prognos.csv" in FNAME:
        
        FILENAME = FOLDERNAME + FNAME
        DATA_FOLDER = ""

        OUTPUT_LOC = FNAME + '_logs'
        DATA_PROCESSED_FOLDER = path.join(DATA_FOLDER, OUTPUT_LOC)
        os.makedirs(DATA_PROCESSED_FOLDER, exist_ok=True)


        df = pd.read_csv(FILENAME, low_memory=False, header=None)
        df_seq_list = df.to_numpy()

        df_seq_list = [remove_nan_object(seq) for seq in df_seq_list]
        
        df_env = Environment(debug=DEBUG, log_path=DATA_PROCESSED_FOLDER)
        print("HOs on 4G Radio Interface")
        print("Currently working on: ", FNAME)
        df_env.run_lte(df_seq_list)

        print("Logs saved to log file")

