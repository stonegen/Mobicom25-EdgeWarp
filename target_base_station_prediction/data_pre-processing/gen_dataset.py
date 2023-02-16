# OBJECTIVES:
# Generate the dataset in a way that can be used directly by prognos & BSP

# imports
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
import os
import csv

def main():
    gen_event_ho_combo()


def gen_event_ho_combo():

    # The format accepted by the functions has been explained here. 

    # You have generated these files using earlier scripts. Please look into the "HO_Events" folder to get an idea of the structure.
    # Format: ["HO_Events/Folder/Category/"]
    # Example: ["HO_Events/FG_HSR/HSR_July/", "HO_Events/FG_HSR/HSR_August/"]
    paths = []

    # You have generated these files using earlier scripts. Please look into the "parsed_xmls" folder to get an idea of the structure.
    # Format: ["parsed_xmls/Folder/Category/"]
    # Example: ["parsed_xmls/FG_HSR/HSR_July/", "parsed_xmls/FG_HSR/HSR_August/"]
    meas_paths = []

    # You have generated these files using earlier scripts. Please look into the "Events" folder to get an idea of the structure.
    # Format: ["Events/Folder/Category"]
    # Example: ["Events/FG_HSR/HSR_July", "Events/FG_HSR/HSR_August"]
    events_paths = []

    # Format: [filename.csv] , any csv file title works.
    # These files contain LTE events for Prognos, along with the path to the file containg the related signal strength measurements
    # Example: ["HSR_July_combined.csv", "HSR_August_combined.csv"]
    combined_datasets = []

    # Format: ["DatasetFolder/Folder"]
    # These folders contain all the data to be used by the Base Station Predictor
    # Example: ["combined_dataset/HSR_July", "combined_Dataset/HSR_August"]
    output_folders = []

    # Format: ["filename.csv"]
    # These files contain the data to be used as input to Prognos.
    # Example: ["HSR_July_prognos.csv", "HSR_August_prognos.csv"]
    prognos_filenames = []

    # Format: ["filename.csv"]
    # These files contain timestamps for when LTE Events are reported, and the actual handover takes place
    # Example: ["HSR_July_ts.csv", "HSR_August_ts.csv"]
    prognos_ts_filenames = []

    for i in range(len(paths)):
        
        path = paths[i]
        meas_path = meas_paths[i]
        events_folder_path = events_paths[i]
        combined_dataset_fpath = combined_datasets[i]
        prognos_filename = prognos_filenames[i]
        timestamps_filename = prognos_ts_filenames[i]
        output_folder = output_folders[i]

        all_ho_events, all_events = generate_pairs(path, meas_path, events_folder_path)

        extract_related_events(all_ho_events, all_events, combined_dataset_fpath)

        generate_for_prognos(combined_dataset_fpath, prognos_filename)

        extract_timestamps(combined_dataset_fpath, timestamps_filename)

        extract_model2(combined_dataset_fpath, output_folder)



# This function extracts the dataset in a format that can be used pretty much directly by model 2
def extract_model2(lte_ho_combined_fpath, output_folder):
    df = pd.read_csv(lte_ho_combined_fpath)
    df.drop("Unnamed: 0", axis=1, inplace=True)
    
    all_lengths = []

    for i in range(len(df)):
        entry = df.iloc[i]

        # This line can be modified to decide where to save the result
        # dest_fpath is the destination folder name, not file name - unique for each handover event
        dest_fpath = output_folder + "/" + str(i)
        try:
            os.makedirs(dest_fpath)
        except:
            pass
        
        # Extract the timeseries, and write to the destination file
        extract_timeseries(entry, dest_fpath)


# Extract timeseries, entry gives relevant information, and index tells where to extract to
def extract_timeseries(entry, dest_fpath):
    # Initial variable setup
    lte_event_ts = entry.lte_event_ts
    ho_event_ts = entry.ho_event_ts
    file_loc = entry.source_fname
    target_bs = entry.target_bs
    lte_event_ts = string_to_datetime(lte_event_ts)
    ho_event_ts = string_to_datetime(ho_event_ts)
    source_meas_df = pd.read_csv(file_loc)
    
    # Generate timestamps as datetime objects
    timestamps = [string_to_datetime(x) for x in source_meas_df.timestamp]    
    source_meas_df["timestamp_dt"] = timestamps
    max_nbr_cells = max(source_meas_df["Number of Neighbor Cells"])
    
    # TODO: Possibly vary this to see if datasets with longer histories can give better results
    half_second = timedelta(milliseconds=500)
    start_time = lte_event_ts - half_second

    filtered_df = source_meas_df[source_meas_df.timestamp_dt >  start_time]
    filtered_df = filtered_df[source_meas_df.timestamp_dt < ho_event_ts]

    # This function call returns the list of all neighbors, along with dictionaries for each neighbor's measurements and measurement timestamps
    all_nbrs, nbr_rsrp_dict, nbr_rsrq_dict, nbr_timestamp_dict, nbr_label_dict, nbr_rsrp_raw_dict, nbr_rsrq_raw_dict = extract_differences(filtered_df, max_nbr_cells, target_bs)

    # Write a function to write all of this data into relevant files to make the dataset
    write_to_file(dest_fpath, all_nbrs, nbr_rsrp_dict, nbr_rsrq_dict, nbr_timestamp_dict, nbr_label_dict, nbr_rsrp_raw_dict, nbr_rsrq_raw_dict)

    

# Function to write the extracted data to a file
def write_to_file(dest_fpath, all_nbrs, nbr_rsrp_dict, nbr_rsrq_dict, nbr_timestamp_dict, nbr_label_dict, nbr_rsrp_raw_dict, nbr_rsrq_raw_dict):
    for i, nbr in enumerate(all_nbrs):
        
        # Extract data
        rsrp_list = nbr_rsrp_dict[nbr]
        rsrq_list = nbr_rsrq_dict[nbr]
        timestamp_list = nbr_timestamp_dict[nbr]
        label_list = nbr_label_dict[nbr]
        rsrp_raw_list = nbr_rsrp_raw_dict[nbr]
        rsrq_raw_list = nbr_rsrq_raw_dict[nbr]

        # Create dataframe
        df = pd.DataFrame()
        df["timestamp"] = timestamp_list
        df["rsrp"] = rsrp_list
        df["rsrq"] = rsrq_list
        df["rsrp_raw"] = rsrp_raw_list
        df["rsrq_raw"] = rsrq_raw_list
        df["label"] = label_list
        
        # Write to destination file
        dest_file = dest_fpath + "/" + str(i) + ".csv"
        df.to_csv(dest_file)

def extract_differences(df, max_nbr_cells, target_bs):
    serving_ids = list(df["Serving Physical Cell ID"])
    serving_rsrp = list(df["RSRP(dBm)"])
    serving_rsrq = list(df["RSRQ(dB)"])

    nbr_timestamp_dict = {}
    nbr_rsrp_dict = {}
    nbr_rsrq_dict = {}
    nbr_label_dict = {}
    # Adding for Raw measurements
    nbr_rsrq_raw_dict = {}
    nbr_rsrp_raw_dict = {}

    # List containing ids of all neighbors in this df
    all_nbrs = []

    for i in range(len(df)):
        # make per neighbor measurement arrays
        entry = df.iloc[i]
        num_nbrs = entry["Number of Neighbor Cells"]
        
        # Get current serving cell measurements as well
        curr_serving_rsrp = serving_rsrp[i]
        curr_serving_rsrq = serving_rsrq[i]

        for j in range(num_nbrs):
            # Extracting current neighbor cell
            curr_nbr = None

            if j == 0:
                curr_nbr = entry["Physical Cell ID"]
            else:
                index_str = "Physical Cell ID." + str(j)
                curr_nbr = entry[index_str] 
            
            # Add to all neighbor list if not already there
            if curr_nbr not in all_nbrs:
                all_nbrs.append(curr_nbr)

            # This should cover the case of any entry not being present in the dict, and a new list will be spawned for the respective neighbor
            try:
                nbr_rsrp_dict[curr_nbr]
            except:
                nbr_rsrp_dict[curr_nbr] = []
                nbr_rsrq_dict[curr_nbr] = []
                nbr_timestamp_dict[curr_nbr] = []
                nbr_label_dict[curr_nbr] = []
                # Adding for Raw measurements
                nbr_rsrp_raw_dict[curr_nbr] = []
                nbr_rsrq_raw_dict[curr_nbr] = []

            # Extracting current neighbor rsrp, rsrq, and the relevant timestamp entry
            rsrp_str = "RSRP(dBm)." + str(j+1)
            rsrq_str = "RSRQ(dB)." + str(j+1)
            curr_rsrp = entry[rsrp_str]
            curr_rsrq = entry[rsrq_str]
            curr_timestamp = entry["timestamp"]

            # Evaluating differences
            rsrp_diff = curr_rsrp - curr_serving_rsrp
            rsrq_diff = curr_rsrq - curr_serving_rsrq


            # TODO: Return the raw rsrp and rsrq values as well

            # Check if equal to target, we will use these as labels
            if curr_nbr == target_bs:
                nbr_label_dict[curr_nbr].append("target")
            else:
                nbr_label_dict[curr_nbr].append("not_target")

            # saving in dicts
            nbr_rsrp_dict[curr_nbr].append(rsrp_diff)
            nbr_rsrq_dict[curr_nbr].append(rsrq_diff)
            nbr_timestamp_dict[curr_nbr].append(curr_timestamp)
            nbr_rsrp_raw_dict[curr_nbr].append(curr_rsrp)
            nbr_rsrq_raw_dict[curr_nbr].append(curr_rsrq)


    return all_nbrs, nbr_rsrp_dict, nbr_rsrq_dict, nbr_timestamp_dict, nbr_label_dict, nbr_rsrp_raw_dict, nbr_rsrq_raw_dict


# Extracts timestamps only (of LTE events and handover events)
def extract_timestamps(filepath, output_filename):
    df = pd.read_csv(filepath)
    df.drop("Unnamed: 0", axis=1, inplace=True)
    df.drop("index", axis=1, inplace=True)

    final_df = pd.DataFrame()
    final_df["lte_ts"] = df["lte_event_ts"]
    final_df["ho_ts"] = df["ho_event_ts"]

    # final_df.to_csv("prognos_timestamps.csv")
    final_df.to_csv(output_filename)


# Generates dataset specifically in the format required by prognos
def generate_for_prognos(filepath, output_filename):
    # dataframe to load
    df = pd.read_csv(filepath)
    df.drop("Unnamed: 0", axis=1, inplace=True)
    df.drop("index", axis=1, inplace=True)

    lte_events = df["events"]
    final_strings_list = [] # This will contain all the strings that have to be written to the final csv
    for event_set in lte_events:
        event_set = event_set.strip("][").split(', ') # need to do this to remove brackets and stuff

        curr_str = "" # will be appending this string into our final csv
        for event in event_set:
            event = event.strip("''")
            curr_str += event
            curr_str += ","
        curr_str += "pcell_intra"
        final_strings_list.append(curr_str)

    # with open("prognos_dataset.csv", "w") as fp:
    with open(output_filename, "w") as fp:
        for item in final_strings_list:
            fp.write(item)
            fp.write("\n")

## Extracting relevant events from the handover event dataframes and lte events
def extract_related_events(all_ho_events, all_events, output_fname):
    lte_event_counter = 0
    ho_event_counter = 0
    num_ho_events = len(all_ho_events)
    num_lte_events = len(all_events)
    relevant_pairs = None

    while (lte_event_counter < num_lte_events and ho_event_counter < num_ho_events):
        lte_event = all_events.iloc[lte_event_counter]
        ho_event = all_ho_events.iloc[ho_event_counter]
        time_diff = (ho_event["dt_timestamps"] - lte_event["dt_timestamps"]).total_seconds()
        # print(time_diff)
        if time_diff < 2.5 and time_diff >= 0:
            # Combine the dataframes for handover events and lte events
            combined = lte_event[["dt_timestamps", "events"]]
            combined.rename({"dt_timestamps": "lte_event_ts"}, inplace=True)
            combined2 = ho_event[["dt_timestamps", "source_fname", "source_bs", "target_bs"]]
            combined2.rename({"dt_timestamps": "ho_event_ts"}, inplace=True)

            # preprocessing dataframe
            final_df = pd.concat([combined, combined2], axis=0)
            final_df = final_df.reset_index().T
            final_df.columns = ["lte_event_ts", "events", "ho_event_ts", "source_fname", "source_bs", "target_bs"]
            final_df = final_df.drop(["index"])

            # Combining releavnt extract details from handover file and from lte event file
            if relevant_pairs is None:
                relevant_pairs = final_df
            else:
                relevant_pairs = pd.concat([relevant_pairs, final_df])
            lte_event_counter += 1
        elif time_diff < 0:
            ho_event_counter += 1
        elif time_diff > 2.5:
            lte_event_counter += 1

    relevant_pairs.reset_index(inplace=True)
    # output_fname = "lte_ho_combined.csv"
    relevant_pairs.to_csv(output_fname, encoding='utf-8')


## Generating dataset for prognos evaluation, but not in the exact format that it needs. We will cut down on this dataset for prognos.
def generate_pairs(ho_folder_path, meas_folder_path, events_folder_path):
    all_ho_events = None
    # ho_folder_path = "HO_Events/LowMobi/Sprint"
    ho_files = sorted(os.listdir(ho_folder_path))
    for ho_file in ho_files:
        ho_fpath = ho_folder_path+"/"+ho_file+"/handovers.csv"
        # source_file_name = "../parsed_xmls/LowMobi/Sprint/" + ho_file + "/intra_freq.csv"
        source_file_name = meas_folder_path + ho_file + "/intra_freq.csv"
        # dataframe for current handovers file
        ho_df = pd.read_csv(ho_fpath)

        # Connvert datetime string to datetime object
        datetime_obj_list = [string_to_datetime(x) for x in ho_df["timestamps"]]

        # entry to be added to the dataframe
        fname_entry_list = [source_file_name for x in range(len(ho_df))]
        ho_df["source_fname"] = fname_entry_list
        ho_df["dt_timestamps"] = datetime_obj_list
        if all_ho_events is None:
            all_ho_events = ho_df
        else:
            all_ho_events = pd.concat([all_ho_events, ho_df])

    # events_folder_path = "Events/Low-mobility/Sprint"
    events_files = sorted(os.listdir(events_folder_path))
    all_events = None
    for event_file in events_files:
        event_fpath = events_folder_path + "/" + event_file
        event_df = pd.read_csv(event_fpath)

        datetime_obj_list = [string_to_datetime(x) for x in event_df["timestamp"]]
        event_df["dt_timestamps"] = datetime_obj_list

        if all_events is None:
            all_events = event_df
        else:
            all_events = pd.concat([all_events, event_df])

    all_events.sort_values("dt_timestamps")
    all_ho_events.sort_values("dt_timestamps")
    all_events.reset_index(inplace=True)
    all_ho_events.reset_index(inplace=True)

    return all_ho_events, all_events


def string_to_datetime(timestamp_str):
    try:
        time_b = datetime.strptime(timestamp_str, "%Y-%m-%d %X.%f")
    except:
        time_b = datetime.strptime(timestamp_str, "%Y-%m-%d %X")
    return time_b


def gen_timeseries():
    master_ts_list = [] # contains all required timestamps

    lte_subfolders = os.listdir("Events")
    for f1 in lte_subfolders:
        sf_path = "Events/" + f1
        carriers = os.listdir(sf_path)
        for f2 in carriers:
            # Getting file paths sorted
            f2_path = sf_path + "/" + f2
            f3s = os.listdir(f2_path)
            previous_entry = None
            f3s = sorted(f3s)
            file_count = len(f3s)
            # Initializing variables to be carried on
            se = {}
            se["start"] = None
            se["end"] = None
            prev_end = None
            for f_ind, f3 in enumerate(f3s):
                f3_path = f2_path + "/" + f3 # LTE events file path
                ho_f_path = "HO_Events/" + f1 + "/" + f2 + "/" + f3[:-4] + "/" + "handovers.csv" # handover events file path , might not need this though, since we are only logging start and end times

                measurements_path = "../parsed_xmls/" + f1 + "/" + f2 + "/" + str(f_ind) + "/intra_freq.csv"

                if os.path.exists(f3_path) and os.path.exists(ho_f_path) and os.path.exists(measurements_path):

                    meas_df = pd.read_csv(measurements_path) # contains all measurements
                    ho_df = pd.read_csv(ho_f_path) # contains handover events only
                    lte_df = pd.read_csv(f3_path) # contains lte events

                    ho_tss = list(ho_df["timestamps"]) # handover event timestamps
                    lte_tss = list(lte_df["timestamp"]) # lte event timestamps
                    ho_source_bs_list = list(ho_df["source_bs"]) # handovers source bs
                    ho_target_bs_list = list(ho_df["target_bs"]) # handovers target bs

                    event_timestamps = []
                    corr_handovers = [] # corresponding handovers for every event timestamp
                    corr_ho_sbs = [] # list containing corresponding handover event source base stations
                    corr_ho_tbs = [] # list containing corresponding handover event target base stations
                    ho_index = 0

                    # Starting over
                    print(ho_tss)
                    print(lte_tss)

                    quit()


def gen_filewise():
    # OBJECTIVE:
    # This function generates the dataset on a per-file basis.
    # lte_events
    master_event_list = []
    lte_subfolders = os.listdir("Events")
    for f1 in lte_subfolders:
        sf_path = "Events/" + f1
        carriers = os.listdir(sf_path)
        for f2 in carriers:
            # Getting file paths sorted
            f2_path = sf_path + "/" + f2
            f3s = os.listdir(f2_path)
            previous_entry = None
            f3s = sorted(f3s)
            file_count = len(f3s)
            # Initializing variables to be carried on
            se = {}
            se["start"] = None
            se["end"] = None
            prev_end = None
            for f_ind, f3 in enumerate(f3s):
                f3_path = f2_path + "/" + f3 # LTE events file path
                ho_f_path = "HO_Events/" + f1 + "/" + f2 + "/" + f3[:-4] + "/" + "handovers.csv" # handover events file path , might not need this though, since we are only logging start and end times
                # print(ho_f_path)
                # print(f3[:-4])
                if os.path.exists(f3_path) and os.path.exists(ho_f_path):

                    ho_df = pd.read_csv(ho_f_path)
                    lte_df = pd.read_csv(f3_path)

                    ho_tss = list(ho_df["timestamps"])
                    lte_tss = list(lte_df["timestamp"])
                    # print(ho_tss)
                    # print(lte_tss)

                    ho_index = 0
                    for i, ts in enumerate(lte_tss):
                        if ho_index < len(ho_tss):
                            ho_ts = ho_tss[ho_index]
                            ho_ts = ho_tss[ho_index]
                            time_diff = get_time_diff(ho_ts, ts)
                            if time_diff > 0:
                                if time_diff < 2.5:
                                    entry = lte_df.iloc[i]
                                    events = entry["events"]
                                    # print(events)
                                    master_event_list.append(events)
                            while time_diff < 0:
                                ho_index += 1
                                if ho_index < len(ho_tss):
                                    ho_ts = ho_tss[ho_index]
                                    time_diff = get_time_diff(ho_ts, ts)
                                    if time_diff > 0:
                                        if time_diff < 2.5:
                                            entry = lte_df.iloc[i]
                                            events = entry["events"]
                                            # print(events)
                                            master_event_list.append(events)
                                else:
                                    break
                        else:
                            break


    f = open("lte_dataset.csv", "w")
    writer = csv.writer(f)
    for event_list in master_event_list:
        entry = []
        # print(event_list)
        event_list = event_list.strip('[]')
        events = event_list.split(', ')
        for event in events:
            event = event.strip("'")
            entry.append(event)
        # print(entry)
        entry.append("pcell_intra")
        writer.writerow(entry)
    f.close()

def conv_time(t):
    # converting to datetime objects
    time_b = None
    try:
        time_b = datetime.strptime(t, "%Y-%m-%d %X.%f")
    except:
        time_b = datetime.strptime(t, "%Y-%m-%d %X")
    t_sec = int(time.mktime(time_b.timetuple()))
    t_sec = t_sec + (time_b.microsecond / 1000000)
    return t_sec

    # return time_b

def get_time_diff(time_a, time_b):

    try:
        time_b = datetime.strptime(time_b, "%Y-%m-%d %X.%f")
    except:
        time_b = datetime.strptime(time_b, "%Y-%m-%d %X")

    try:
        time_a = datetime.strptime(time_a, "%Y-%m-%d %X.%f")
    except:
        time_a = datetime.strptime(time_a, "%Y-%m-%d %X")

    # calculating time difference
    handover_duration = time_a - time_b
    handover_duration = handover_duration.total_seconds()
    return handover_duration
    # return abs(handover_duration)

if __name__ == "__main__":
    main()
