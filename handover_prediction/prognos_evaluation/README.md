# Prognos Evaluation

Prognos is the solution used by EdgeCat for predicting whether or not a handover will occur. Prognos makes use of LTE and 5G measurement reports/events sequences to predict whether or not a handover event is about to take place. We use Prognos as a trigger to begin predicting the target base station.

## Setup

We use the original authors' implementation of Prognos, which can be found as part of their paper's [artifact](https://github.com/SIGCOMM22-5GMobility/artifact). 

Duplicate the above repository on your machine. You are also required to install python3, and are recommended to set up and activate a virtual environment, you can find more information on how to do that [here](https://pythonbasics.org/virtualenv/). 

Activate your virtual environment, `cd` into the repository and run the following command to set up your virtual environment.

```bash
pip3 install -r requirements.txt
```

You will then need to copy two folders into the current folder (`prognos_evaluation/`). The two folders are:

1. `artifact/utils/`
2. `artifact/src/common/`

## Evaluation Script

After setup, you can run the evaluation script, `prognos_evaluation.py`, evaluating prognos. You will have to make one modification to the script. Open `prognos_evaluation.py` in a text editor, and modify the `FOLDERNAME` variable, and set it equal to the location of the folder containing input files for Prognos's evaluation. 

The script assumes that the filenames for the input files will have the following format: 
```bash
filename_prognos.csv
```

## Input Shape Needed by Prognos

For a sequence of events to be analyzed by prognos, the data in the `*_prognos.csv` file should have the following structure: a single line should contain **comma separated LTE events**, and the last entry contains the **handover type**. 

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
