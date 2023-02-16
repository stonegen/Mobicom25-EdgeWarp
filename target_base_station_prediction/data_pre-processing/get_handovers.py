import os
import pandas as pd
from datetime import datetime
import csv

def main():
    extract_handovers()

def extract_handovers():
    # this function recurses into the required folders with the input data, and also generates the output folders/paths for the final result as well
    # It passes the information to a helper function to do the actual processing we need

    counter = 0
    master_folder_path = "parsed_xmls"
    l1_folders = os.listdir(master_folder_path)
    for f1 in l1_folders:
        f2_path = master_folder_path + "/" + f1
        l2_folders = os.listdir(f2_path)
        o1_path = "HO_Events/" + f1
        try:
            os.mkdir(o1_path)
        except:
            pass
        for f2 in l2_folders:
            o2_path = o1_path + "/" + f2
            try:
                os.mkdir(o2_path)
            except:
                pass
            f3_path = f2_path + "/" + f2
            l3_folders = os.listdir(f3_path)
            l3_folders = sorted(l3_folders)
            for f3 in l3_folders:            
                f4_path = f3_path + "/" + f3
                # extracting only intra_freq.csv files
                file_path = f4_path + "/" + "intra_freq.csv"
                if os.path.exists(file_path):
                    o3_path = "HO_Events/" + f1 + "/" + f2 + "/" + f3 
                    try:
                        os.mkdir(o3_path)
                    except:
                        pass
                    get_ho_events(file_path, o3_path) 
                    print(counter, " files done")
                    counter += 1
                

def get_ho_events(input_file, output_folder):
    output_path = output_folder + "/handovers.csv"
    df = pd.read_csv(input_file)
    handovers_only = df[df.handover == True]
    timestamps = handovers_only.timestamp
    ts_indices = timestamps.index # timestamp indices
    ts_indices = list(ts_indices)
    timestamps = timestamps.values.tolist()
    timestamps_final = []
    final_indices = []
    source_bs = []
    target_bs = []  
    labels = ["timestamps", "original_indices", "source_bs", "target_bs"]
    entries = []

    for i, index in enumerate(ts_indices):
        before_entry = df.iloc[index-1]
        after_entry = df.iloc[index]
        time_bstring = before_entry["timestamp"] # b stands for before
        time_a = after_entry["timestamp"] # a stands for after
        # I didn't realize until later how confusing time_b and time_a can get
        time_b = None
        # converting to datetime objects
        try:
            time_b = datetime.strptime(time_bstring, "%Y-%m-%d %X.%f")
        except:
            time_b = datetime.strptime(time_bstring, "%Y-%m-%d %X")

        try:
            time_a = datetime.strptime(time_a, "%Y-%m-%d %X.%f")
        except:
            time_a = datetime.strptime(time_a, "%Y-%m-%d %X")
        # calculating time difference
        handover_duration = time_a - time_b
        handover_duration = handover_duration.total_seconds()
        if handover_duration <= 1:
            # print(handover_duration)
            entry = []
            before = before_entry["Serving Physical Cell ID"]
            after = after_entry["Serving Physical Cell ID"]

            entry.append(time_bstring) 
            entry.append(index)
            entry.append(before)
            entry.append(after)
            
            entries.append(entry) 

    with open(output_path, 'w') as csvfile:
        #     writer = csv.DictWriter(csvfile, fieldnames = output_labels)
            writer = csv.writer(csvfile)
            writer.writerow(labels)
            writer.writerows(entries)
         

if __name__ == "__main__":
    main()