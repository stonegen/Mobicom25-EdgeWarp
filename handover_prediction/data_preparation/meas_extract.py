# Running this script is the first step in getting files prepared for Prognos (from MobileInsight logs)

# Importing modules
from mobile_insight.monitor import OfflineReplayer
# from mobile_insight.analyzer import MsgLogger, NrRrcAnalyzer, LteRrcAnalyzer, WcdmaRrcAnalyzer, LteNasAnalyzer, UmtsNasAnalyzer, LteMacAnalyzer, LteMeasurementAnalyzer
from mobile_insight.analyzer import MsgLogger

import xml.etree.ElementTree as ET
import csv
from xml.dom import minidom
import os 
import sys
from tqdm import tqdm

# Importing from our own file
import line_prepender

# This folder contains folders containing should contain all of your mi2log files
# The folder structure should look like this:
# INPUT_FOLDER
# | CATEGORY1
# | | 1.mi2log
# | | 2.mi2log
# | CATEGORY2
# | | 1.mi2log
# | | 2.mi2log
INPUT_FOLDER = ""

# This is a directory which will be created within the RRC_Output folder where your output will be stored
OUTPUT_FOLDER = ""

def make_xml(filename):
    root = minidom.Document()
  
    xml = root.createElement('root') 
    root.appendChild(xml)
    
    xml_str = root.toprettyxml(indent ="\t") 
    
    save_path_file = filename
    
    with open(save_path_file, "w") as f:
        f.write(xml_str) 
    
    # new line added
    f.close()

def extract_mi2log():
    all_folders = []

    logs = os.listdir(INPUT_FOLDER)

    folder_titles = []

    OUTPUT_PATH = "RRC_Output/" + OUTPUT_FOLDER

    try:
        os.mkdir(OUTPUT_PATH)
    except:
        pass
    
    for log in logs:
        full_path = INPUT_FOLDER + "/" + log
        folder_title = OUTPUT_FOLDER + "/" + log
        folder_titles.append(folder_title)
        all_folders.append(full_path)

    for i, folder in enumerate(all_folders):
        extract_from_log(folder, folder_titles[i])

def extract_from_log(input_folder, folder_title):
    print(input_folder)
    print(folder_title)
    src = OfflineReplayer()
    log = "LTE_RRC_OTA_Packet"
    output_folder = "RRC_Output/" + folder_title 
    try:
        os.mkdir(output_folder)
    except:
        pass

    input_files = os.listdir(input_folder)
    input_files = sorted(input_files)

    in_fpaths = []
    for f in input_files:
        path = input_folder + "/" + f
        in_fpaths.append(path)


    for i, input_file in enumerate(in_fpaths):
        
        output_file_name = output_folder + "/" + str(i)
        src.set_input_path(input_file)
        src.enable_log(log)

        logger = MsgLogger()
        logger.set_decode_format(MsgLogger.XML)
        logger.set_dump_type(MsgLogger.FILE_ONLY)
        
        file_name = output_file_name + "_rrc_info.xml"
        make_xml(file_name)
        logger.save_decoded_msg_as(file_name)
        logger.set_source(src)

        src.run()


kv_pairs = {
    0: "A1", 1: "A2", 2:"A3", 3: "A4", 4: "A5", 5: "A6"
}

def decode_xmls():
    input_folder = "RRC_Output"
    subfolders = os.listdir(input_folder)
    for folder in subfolders:
        path = "RRC_Output/" + folder
        output_folder = "Events/" + folder
        try: 
            os.mkdir(output_folder)
        except:
            pass
        level2_f = os.listdir(path) # level 2 folders
        for f2 in level2_f:
            input_path = path + "/" + f2
            output_path = output_folder + "/" + f2
            try:
                os.mkdir(output_path)
            except:
                pass
            print(input_path)
            print(output_path)
            extract_info(input_path, output_path)


def extract_info(input_folder, output_folder):
    input_files = os.listdir(input_folder)
    input_files = sorted(input_files)
    output_labels = ["timestamp", "events"]

    for test_file in tqdm(input_files):
        
        test_path = input_folder + "/" + test_file

        o_fname = test_file.replace("_rrc_info.xml", "") # output file name
        tree = ET.parse(test_path)
        output_fname = output_folder + "/" + o_fname + ".csv"

        final_dict = {}
        forest = tree.getroot()

        entries = []

        for log_packet in forest:
            events = []
            handover = None
            timestamp = log_packet[2].text
            msg_xml = log_packet[-1]
            proto_objs = msg_xml.findall(".//packet//proto")
            for i, proto in enumerate(proto_objs):
                if proto.attrib["name"] == "fake-field-wrapper":
                    for child in proto.iter():
                        if child.attrib["name"] == "lte-rrc.eventId":
                            val = int(child.attrib["showname"][-2])
                            res = kv_pairs[val]
                            events.append(res)
                        elif child.attrib["name"] == "lte-rrc.handoverType":
                            val = child.attrib["showname"][14:]
                            handover = val
        
            # not extracting handover events here
            if (len(events) != 0):
                entries.append([timestamp, events])

        with open(output_fname, 'w') as csvfile:
        
            writer = csv.writer(csvfile)
            writer.writerow(output_labels)
            writer.writerows(entries)
         

if __name__ == "__main__":
    try:
        os.mkdir("RRC_Output")
        os.mkdir("Events")
    except:
        pass


    extract_mi2log()
    line_prepender.main()
    decode_xmls()
