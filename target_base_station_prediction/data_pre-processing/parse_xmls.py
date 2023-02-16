import os
from tqdm import tqdm

def main():
    try:
        os.mkdir("parsed_xmls")
    except:
        pass
    
    # generate input location list
    # generate destinations list
    inputs = []
    inputs2 = []
    inputs3 = []
    inputs4 = []
    dests = []

    # make folders for all subfolders
    types = os.listdir("extracted_data")
    for type in types:
        level1_path = "extracted_data/" + type
        level1 = os.listdir(level1_path)
        for carrier in level1:
            carrier_path = level1_path + "/" + carrier
            files = os.listdir(carrier_path)
            files = sorted(files)
            for log_folder in files:
                log_folder_path = carrier_path + "/" + log_folder
                # print(log_folder_path)
                output_loc = "parsed_xmls/" + type + "/" + carrier + "/" + log_folder
                # print(output_loc)
                try:
                    os.makedirs(output_loc)
                except:
                    pass
                file_name_1 = log_folder_path + "/category1.xml"
                file_name_2 = log_folder_path + "/category2.xml"
                file_name_3 = log_folder_path + "/category3.xml"
                file_name_4 = log_folder_path + "/category4.xml"
                inputs.append(file_name_1)
                inputs2.append(file_name_2)
                inputs3.append(file_name_3)
                inputs4.append(file_name_4)
                dests.append(output_loc)
                

    for i, _ in enumerate(tqdm(dests)):
        os.system("python3 file_prep.py " + inputs[i])
        os.system("python3 file_prep.py " + inputs2[i])
        os.system("python3 file_prep.py " + inputs3[i])
        os.system("python3 file_prep.py " + inputs4[i])
        os.system("python3 xml_parsing.py " + inputs[i] + " " + dests[i])
        os.system("python3 xml_parsing.py " + inputs2[i] + " " + dests[i])
        os.system("python3 xml_parsing.py " + inputs3[i] + " " + dests[i])
        os.system("python3 xml_parsing.py " + inputs4[i] + " " + dests[i])


if __name__ == "__main__":
    main()
