# Target Base Station Prediction

This section contains the code for Target Base Station Prediction, along with the annotated notebook files relevant to all of the analyses. 

## Notebook Files
The notebook files in this section contain the implementation of Base Station Prediction algorithm, and the attached notebooks are directly usable on Google Colab.

### **window_size_discussion.ipynb**
Contains the evaluation of changing the window size on the performance of the model in terms of accuracy, earliest time to prediction (time window between correct target base station prediction and handover event), and dataset usability.

### **model_design_train.ipynb**
Contains dataset preprocessing, model design and model training.

### **Time To Prediction Evaluation.ipynb**
Contains the scripts to evaluate the earliest time to predicting the target BS before handover for the Miscellaneous dataset, and the Driving Scenario dataset.

### **HSR Time To Prediction Evaluation.ipynb**
Contains the scripts to evaluate the earliest time to predicting the target BS before handover for the High Speed Rail dataset.

## Data Folders

### **window_size_data**
The `CDF.txt` files contain line-separated data for the time window between correct base station prediction and handover events (in seconds). 