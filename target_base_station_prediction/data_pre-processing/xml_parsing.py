import xml.etree.ElementTree as ET
import csv
# import xmltodict
import sys
import os

def main():
    # getting filename for source and destination
    file_name = str(sys.argv[1]) # first argument always at index 1. index 0 argument is the name of script itself
    destination = str(sys.argv[2])

    if not os.path.isdir(destination):
        os.mkdir(destination)

    tree = ET.parse(file_name)

    forest = tree.getroot() # we have a kind of forest like arrangement for the xmls

    for tree in forest:
        for item in tree:
            pair = item.attrib
            text = item.text
            key = pair['key']
            if key == "type_id":
                if text == "LTE_PHY_Connected_Mode_Intra_Freq_Meas":
                    intra_freq_meas(forest, destination)
                    return
                elif text == "LTE_PHY_Connected_Mode_Neighbor_Measurement":
                    neighbour_meas(forest, destination)
                    return
                elif text == "LTE_PHY_Serv_Cell_Measurement":
                    serv_cell_meas(forest, destination)
                    return
                elif text == "LTE_RRC_Serv_Cell_Info":
                    serv_cell_info(forest, destination)
                    return

def intra_freq_meas(forest, destination):
    keys = []

    # generate keys:
    valid_indices = [2,5,6,7,8,9,10,11]
    for index,item in enumerate(forest[0]):
        
        pair = item.attrib
        key = pair['key']
        try:
            # print(pair)
            pass
        except:
            pass
        if index in valid_indices:
            keys.append(key)
    
    # initialization for handover detection
    old_id = -1
    new_id = -1
    handover = False
    # keeping a variable to keep track of columns
    total_cols = 0
    num_detected = 0 # number of detected cells
    num_nbrs = 0 # number of neighbor cells
    data = []
    count = 0
    for tree in forest:
        all_nbr_cells = [] # list holding details of nbr cells
        all_detected = [] # list holding details of detected cells
        texts = []
        count += 1
        for index, item in enumerate(tree):
            if index == 6: # check if handover has taken place
                new_id = item.text 
                if (new_id == old_id):
                    handover = False
                else:
                    if (count != 1):
                        handover = True
                        old_id = new_id
                    else:
                        handover = False
                        old_id = new_id
                texts.append(handover)
            if index == 10: # num neighbor cells
                num_nbrs = int(item.text)
            if index == 11: # num detected
                num_detected = int(item.text)
            if index == 12: # neighbour cells
                try:
                # ignore the indexing details. Just going through the xml the way its produced by mobileinsight
                    nbr_cells = item[0] 
                    for cell in nbr_cells:
                        cell = cell[0] # this is the dumbest thing I have ever written, but it works. If indexing is done before this step, then it will skip entries. I do not want to make new variables.
                        cell_details = []
                        for attribute in cell:
                            key = attribute.attrib['key']
                            # print(key)
                            # if key not in keys: 
                            # TODO: Over here, currently it is just adding columns infinitely. We need a better representation  
                            # keys.append(key)
                            cell_details.append(attribute.text)
                        all_nbr_cells.append(cell_details)
                except:
                    pass
            if index == 13: # detected cells
                try:
                    detected_cells = item[0]
                    for cell in detected_cells:
                        cell = cell[0]
                        cell_details = []
                        for attribute in cell:
                            key = attribute.attrib['key']
                            # if key not in keys:
                            # TODO: Over here, currently it is just adding columns infinitely. We need a better representation
                            # keys.append(key)
                            cell_details.append(attribute.text)
                        all_detected.append(cell_details)
                except:
                    pass

            if index in valid_indices:
                text = item.text
                texts.append(text)
            
            # adjusting total number of columns to add in the end
            total_cols = max([total_cols, num_detected+num_nbrs])
            
            # adding the neighbour and detected cells info
        for nbr_data in all_nbr_cells:
            for attrib in nbr_data:
                texts.append(attrib)
        for detected_data in all_detected:
            for attrib in detected_data:
                texts.append(attrib)
            # TODO: Get a better representation for this data
        data.append(texts)
    
    # adding appropriate number of columns to keys
    for i in range(total_cols):
        keys.append("Physical Cell ID")
        keys.append("RSRP(dBm)")
        keys.append("RSRQ(dB)")

    keys.insert(2, "handover")
    dest_filename = destination + '/intra_freq.csv'
    with open(dest_filename, 'w', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(keys)
        writer.writerows(data)


def neighbour_meas(forest, destination):
    keys = []

    valid_indices = [2]
    for index, item in enumerate(forest[0]):
        pair = item.attrib
        try:
            item_type = pair['type']
        except:
            pass
        key = pair['key']
        if index in valid_indices:
            keys.append(key)

    data = []

    neighbour_measurements = []    

    for tree in forest:
        texts = []
        for index, item in enumerate(tree):
            if index == 5:
                try:
                    subpacket_1 = item[0][1]
                    key = subpacket_1[0][4].attrib['key']
                    if key not in keys:
                        keys.append(key)
                    texts.append(subpacket_1[0][4].text)  

                    valid_prop_indices = [0,2,3,4]
                    # neighbour cells at 7th index
                    neighbours = subpacket_1[0][7][0]
                    for index, neighbour in enumerate(neighbours):
                        nbr_props = neighbour[0] # neighbour properties
                        curr_nbr_props = []
                        for index, prop in enumerate(nbr_props):
                            if index in valid_prop_indices:
                                key = prop.attrib['key']
                                if key not in keys:
                                    keys.append(key)
                                curr_nbr_props.append(prop.text)
                        neighbour_measurements.append(curr_nbr_props)
                except:
                    pass
            if index in valid_indices:
                text = item.text
                texts.append(text)
        for measurement in neighbour_measurements:
            new_texts = texts + measurement
            data.append(new_texts)

    dest_filename = destination + "/neighbour_meas.csv"
    with open(dest_filename, 'w', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(keys)
        writer.writerows(data)
    
 
def serv_cell_meas(forest, destination):
    keys = []
    valid_indices = [2,4]

    for index, item in enumerate(forest[0]):
        pair = item.attrib
        if index in valid_indices:
            pair = item.attrib
            key = pair['key']
            keys.append(key)

    data = [] 
    subpacket_indices = [4,8,9,10,11,12,13,14,15,16,17,18]
    for tree in forest:
        texts = []
        for index, item in enumerate(tree):
            # print(tree)
            if index in valid_indices:
                text = item.text
                texts.append(text)
            if index == 5:
                for subpacket in item[0]:
                    for subpack_index in subpacket_indices:
                        key = subpacket[0][subpack_index].attrib['key']
                        if key not in keys:
                            keys.append(key)
                        texts.append(subpacket[0][subpack_index].text)

        data.append(texts)

    dest_filename = destination + "/serv_cell_meas.csv"
    with open(dest_filename, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(keys)
        writer.writerows(data)


def serv_cell_info(forest, destination):
    keys = []
    valid_indices = [2,5,6,7,8,9,11,12,14]
    for index, item in enumerate(forest[0]):
        if index in valid_indices:
            pair = item.attrib
            key = pair['key']
            # if index in valid_indices:
            keys.append(key)
        data = [] 
    for tree in forest:
        texts = []
        for index, item in enumerate(tree):

            if index in valid_indices:
                text = item.text
                texts.append(text)

        data.append(texts)

    dest_filename = destination + "/serv_cell_info.csv"
    with open(dest_filename, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(keys)
        writer.writerows(data)

if __name__ == "__main__":
    main()
