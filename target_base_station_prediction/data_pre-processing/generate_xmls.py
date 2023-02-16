import os
# Execute this script to parse 

def main():
    # generate commands
    commands = []
    # This folder contains folders containing should contain all of your mi2log files
    # The folder structure should look like this:
    # INPUT_FOLDER
    # | CATEGORY1
    # | | 1.mi2log
    # | | 2.mi2log
    # | CATEGORY2
    # | | 1.mi2log
    # | | 2.mi2log
    
    INPUT_PATH = ""
    OUTPUT_FOLDER = ""
    carriers = os.listdir(INPUT_PATH)

    try:
        os.mkdir("extracted_data")
    except:
        pass
    try:
        os.mkdir("extracted_data/" + carriers)
    except:
        pass

    for carrier in carriers:
        carrier_path = INPUT_PATH + "/" + carrier
        try:
            carrier_dest = "extracted_data/" + OUTPUT_FOLDER + "/" + carrier
            os.mkdir(carrier_dest)
        except:
            pass
        logs = os.listdir(carrier_path)
        logs = sorted(logs)
        for index, log in enumerate(logs):
            dest_path = carrier_dest + "/" + str(index)
            try:
                os.mkdir(dest_path)
            except:
                pass
            log_path = carrier_path + "/" + log
            command = "python3 data_extraction.py " + log_path + " " + dest_path
            commands.append(command)

    for command in commands:
        os.system(command)

if __name__ == "__main__":
    main()