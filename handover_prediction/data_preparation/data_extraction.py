# To run this file in standalone mode, use the following format:
# python3 data_extraction.py INPUT_FILENAME.mi2log OUTPUT_FOLDER_NAME

# Importing modules
from mobile_insight.monitor import OfflineReplayer
from mobile_insight.analyzer import MsgLogger, NrRrcAnalyzer, LteRrcAnalyzer, WcdmaRrcAnalyzer, LteNasAnalyzer, UmtsNasAnalyzer, LteMacAnalyzer, LteMeasurementAnalyzer
from xml.dom import minidom
import os 
import sys


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

extraction_list = ["LTE_PHY_Connected_Mode_Intra_Freq_Meas", "LTE_PHY_Connected_Mode_Neighbor_Measurement", "LTE_PHY_Serv_Cell_Measurement", "LTE_RRC_Serv_Cell_Info"]

def main():
    for i,log in enumerate(extraction_list):
        src = OfflineReplayer()
        # to extract from multiple files, simply add them all to a folder and add the folder's path below
        input_file = str(sys.argv[1]) # first arg is input filename
        output_folder_name = str(sys.argv[2]) # second argument is name of output folder

        src.set_input_path(input_file)
        src.enable_log(log)

        logger = MsgLogger()
        logger.set_decode_format(MsgLogger.XML)
        logger.set_dump_type(MsgLogger.FILE_ONLY)
        file_name = output_folder_name + "/category" + str(i+1) + ".xml"
        make_xml(file_name)
        logger.save_decoded_msg_as(file_name)
        logger.set_source(src)

        src.run()

# to extract from multiple files, simply add them all to a folder and add the folder's path
# also adjust file name to reflect what folder they were all extracted from

if __name__ == "__main__":
    main()
