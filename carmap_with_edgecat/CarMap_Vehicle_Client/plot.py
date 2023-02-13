import pandas as pd
import plotly.express as px
import csv
import numpy as np
from matplotlib import pyplot as plt
import matplotlib

def GetCol(fileName, columNumber):
    # opening the csv file by specifying
    # the location
    # with the variable name as csv_file
    with open(fileName) as csv_file:
    
        # creating an object of csv reader
        # with the delimiter as ,
        csv_reader = csv.reader(csv_file, delimiter = ',')
    
        # list to store the names of columns
        list_of_column_names = []
    
        # loop to iterate through the rows of csv
        for row in csv_reader:
    
            # adding the first row
            list_of_column_names.append(row)
    
    data =  np.array([row[columNumber] for row in list_of_column_names], dtype='float64')
    dataSize = len(data)
    data = data * 1000 # Convert from seconds to ms.

    return data[2:dataSize], dataSize-2

subFolder = '1s'
inputFolderName = "build/" + subFolder + "/"
outputFileName = "CarMap_RTT_Comparison_" + subFolder + ".png" 


modifiedSchemeMs, dataSize = GetCol(inputFolderName + 'modified_e22_delay.csv', 4)
xModifiedScheme = np.array(range(1, dataSize + 1))

defaultDynamicOnlyMs, dataSize = GetCol(inputFolderName + 'blocking_dynamic_only.csv', 4)
xDefaultSchemeBlockingOnly = np.array(range(1, dataSize + 1))

defaultAllStateMs, dataSize = GetCol(inputFolderName + 'blocking_all_state.csv', 4)
xDefaultSchemeAll = np.array(range(1, dataSize + 1))



font = {'family' : 'DejaVu Sans',
        'weight' : 'normal',
        'size'   : 14}

matplotlib.rc('font', **font)

fig, ax = plt.subplots(figsize=(6,2.5))

plt.plot(xModifiedScheme, modifiedSchemeMs, 'b',linestyle = 'dashdot', label="CarMap with EdgeCat")
# plt.plot(xDefaultSchemeBlockingOnly, defaultDynamicOnlyMs, '--g', label="Reactive Migration Optimized")
plt.plot(xDefaultSchemeAll, defaultAllStateMs, 'r', linestyle = ':', label="CarMap Default")
ax.set_xlim((min(xModifiedScheme), max(xModifiedScheme) + 1)) 
ax.set_xticks(np.arange(min(xModifiedScheme), max(xModifiedScheme) + 1, 17))

# # ax.set_ylim([0, 160])
ax.set_yticks(np.arange(int(min(defaultAllStateMs)), int(max(defaultAllStateMs)+1), 15))

# Put a legend below current axis
plt.legend(loc='upper center', bbox_to_anchor=(0.7, 0.95),ncol=1)
ax.set_xlabel('Packet Number')
ax.set_ylabel('RTT (ms)')
# plt.legend("Edge Cat", "Reactive Migration Optimized", "Reactive Migration Base Case")
# plt.legend("Edge Cat", "Reactive Migration Optimized")
plt.xlim(1, dataSize)
plt.grid(which='both', color='grey', linestyle='--')
plt.tight_layout()
# plt.show()
plt.savefig(outputFileName)