# Data Preparation

This section contains all the files used for the conversion of MobileInsight logs (.mi2log files) into the format used as input by Prognos, and our Base Station Predictor (BSP).

## Structuring your data
For ease of categorization, we assume an easily replicable arrangement of data:
```text
DatasetFolder
|---Category1
|---|---logfile1.mi2log
|---|---logfile2.mi2log
|---|---logfile3.mi2log
|---Category2
|---|---logfile1.mi2log
|---|---logfile2.mi2log
|---|---logfile3.mi2log
...
```

To follow the rest of this guide, you will be required to restructure your files in a similar manner. We will make references to the `DatasetFolder`, and assume that it follows the above structure.

## Requirements
You will need your system to be running Linux or MacOS. This is mainly because you will be required to install [MobileInsight](http://www.mobileinsight.net/download.html), which only provides support for MacOS and Linux (specifically Ubuntu). 

You are also required to install python3 on the machine. It is recommended to set up and activate a virtual environment, you can find more information [here](https://pythonbasics.org/virtualenv/). 

Install the required python packages:
```bash
pip3 install -r requirements.txt
```

## MobileInsight installation
You can [download the package](http://www.mobileinsight.net/download.html) and find [installation instructions here](https://github.com/mobile-insight/mobileinsight-core). 

## Execution Instructions for the Base Station Predictor and Prognos

The first file to execute is `generate_xmls.py`. However, you are needed to enter the path to your dataset. Change the `INPUT_PATH` variable to the path of your `DatasetFolder`. Change the `OUTPUT_FOLDER` to an empty folder (or non-existent directory). Then execute the following command in your terminal:

```sh
python3 generate_xmls.py
```
Please note that it is alright to encounter occasional errors for some log files. Logging information can be inconsistent depending on different configurations of the user's mobile device which was used to collect the logs.

This script is used to extract RRC Packet information from `mi2log` files, which is relevant for Prognos. Open this file in a text editor, and edit the `INPUT_FOLDER` variable to the path of your `DatasetFolder`, and the `OUTPUT_FOLDER` variable to anything. Then run the following command in your terminal:
```bash
python3 meas_extract.py
```

After this, you can run the following commands in succession:

```bash
python3 parse_xmls.py
python3 get_handovers.py
```
You will be required to edit some filepaths pertaining to the different categories of your data. Open the `gen_dataset.py` file in a text editor and populate the variables in the `gen_event_ho_combo()` function: `paths`, `meas_paths`, `events_paths`, `combined_datasets`, `output_folders`, `prognos_filenames`, `prognos_ts_filenames`. Lastly, execute this file using the following command:
```bash
python3 gen_dataset.py
```


### To Generate for Prognos
Edit path to folder containing mi2log files in `meas_extract.py`
[TODO: EXPLAIN HOW TO EDIT PATH CORRECTLY]
Run the file using:
```bash
python3 meas_extract.py
```

## Input Shape Needed by Prognos
For a sequence of events to be analyzed by prognos, the data to be analyzed should be placed in a single `.csv` file. A single line should contain **comma separated LTE events**, and the last entry contains the **handover type**. 

**Additional Note**: Prognos determines the maximum number of columns to read using the **first** entry. If Prognos fails to run on automatically generated files, you can fix this error by adding **empty columns (commas)** to the first row in the `csv` file.

Example: 
```txt
A3,A2,A3,pcell_intra,,,,,,,,,,,,,,,,,,,
A5,pcell_intra
A3,A2,A2,A2,A1,pcell_intra
A3,A2,A2,A2,A1,pcell_intra
A5,pcell_intra
A5,pcell_intra
```

## Input Shape Needed by the Base Station Predictor
For a single handover event, we generate separate `csv` files containing signal strength measurements for each base station. These files may be of variable lengths owing to the availability of each base station. 

A file needs the following contents: 
```
timestamp, rsrp, rsrq, rsrp_raw, rsrq_raw, label
```
1. `timestamp` of the measurement
2. `rsrp`: RSRP measurement relative to the current serving cell's RSRP 
3. `rsrq`: RSRQ measurement relative to the current serving cell's RSRQ
4. `rsrp_raw`: Actual RSRP measurement for base station
5. `rsrq_raw`: Actual RSRQ measurement for base station
6. `label`: Whether or not the base station being evaluated is the target base station

Example: A file at `Dataset/Category/HandoverNumber/BaseStationNumber.csv` can have the following contents:

```text
,timestamp,rsrp,rsrq,rsrp_raw,rsrq_raw,label
0,2017-02-27 03:43:22.793750,5.0,2.5625,-78.875,-13.25,target
1,2017-02-27 03:43:22.953750,4.0625,2.25,-78.6875,-13.4375,target
2,2017-02-27 03:43:23.113750,3.5625,1.6875,-78.5,-13.5,target
3,2017-02-27 03:43:23.273750,3.625,2.125,-78.0625,-13.5,target
4,2017-02-27 03:43:23.405000,3.375,2.375,-78.25,-13.625,target
5,2017-02-27 03:43:23.633750,3.375,2.375,-78.25,-13.625,target
6,2017-02-27 03:43:23.653750,4.9375,4.0,-77.125,-13.6875,target
7,2017-02-27 03:43:23.961250,4.9375,4.0,-77.125,-13.6875,target
8,2017-02-27 03:43:23.978750,5.75,5.1875,-78.3125,-13.3125,target
```

The first column (containing indices) is not necessary and can be dropped.