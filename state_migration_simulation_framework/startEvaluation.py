import subprocess
import time
from configuration.config import Config , ConfigParameters
import copy
import signal


# Global list to keep track of the processes
processes = []

def signal_handler(sig, frame):
    print("SIGINT received, terminating subprocesses...")
    for process in processes:
        process.terminate()
    for process in processes:
        process.wait()
    print("Subprocesses terminated.")
    exit(0)



def executeProcesses(experimentConfigParameters : ConfigParameters):

    global processes
    scripts = ["appserver.py", "appclient.py"]
    for script in scripts :
        process = subprocess.Popen(["python3", script])
        processes.append(process)
        time.sleep(5)


    ## Giving some buffer time for our scripts to wind up any extra steps : Precautionary
    time.sleep(experimentConfigParameters.handoverTime + 5)
    
    ## Terminating these processes 
    for process in processes:
        process.terminate()

    for process in processes:
        process.wait()
    processes = []
    

""" 
    THe Following functions are used for the creating the experiments to observe how pre-imptive mobility re
    -lated helps in reducing blocking the state migration.


    A KIND NOTE : Even though These functions will generate you the results you need to be aware that if the configuration of any 2 experiments
    are same then they will end up writing results on the SAME FILE. Hence before running experiments make sure that you run this script for such
    experiments individually and copy out the results in some where else before running the experiments again that have overalapping config 
"""


def runCarMap_Experiment(configuration : Config , defaultConfigParameters : ConfigParameters , WorstCase = 0):

    """
        This experiment will be replicating the Dynamic Nature of the CarMap so that we can observe how the blocking migration time varies
        along with potential reduction due to Preimptive state migration in the background and in built asynchronous Migration in the Redis source
        code In Sha Allah
        Things to note :
        - 3 states will be tested which makes the app state to be around 2.3 MB , 4.3MB , & 5.4 MB
        - hint will be provided 100 ms earlier 
    """
    totalVariables = [11,6,10]
    dynamicVariables = [2,4,6]
    keySize = 10*1024 # This again doesn't matter as the CarMap function in keySaver.py will create keys according to its own distribution
    hint = 10
    handover = 10.1
    
    print(f"Experiment Varying the CAR MAP states has started : ")
    index = 0
    for variables in totalVariables :
        
        experimentConfigParameters = copy.deepcopy(defaultConfigParameters) 
        experimentConfigParameters.totalVariables = variables
        experimentConfigParameters.dynamicVariables = dynamicVariables[index]
        experimentConfigParameters.keySize = keySize
        experimentConfigParameters.appName = "CarMAP_APP"
        experimentConfigParameters.handoverTime = handover
        experimentConfigParameters.hintTime = hint
        experimentConfigParameters.WorstCase = WorstCase
        configuration.setConfigParameters(experimentConfigParameters)
        executeProcesses(experimentConfigParameters)
        index +=1 

    print(f"Experiment Varying the CAR MAP has Ended")
    configuration.setConfigParameters(defaultConfigParameters) 


def runEMP_Experiment(configuration : Config , defaultConfigParameters : ConfigParameters , WorstCase = 0) :
    
    """
        In this experiment we will be having 3 different dynamic states to observe improvements in
        blocking statemigration In Sha Allah
        Things to note :
            - total variables = dynamic variables 
            - key size 32 kb 
    """

    totalVariables = [15, 23 , 31]
    keySize = (32 * 1024)
    print(f"Experiment Varying the EMP states has started : ")
    for variables in totalVariables :

        experimentConfigParameters = copy.deepcopy(defaultConfigParameters) 
        experimentConfigParameters.totalVariables = variables
        experimentConfigParameters.dynamicVariables = variables
        experimentConfigParameters.keySize = keySize
        experimentConfigParameters.appName = "EMP_APP"
        experimentConfigParameters.WorstCase = WorstCase
        configuration.setConfigParameters(experimentConfigParameters)
        executeProcesses(experimentConfigParameters)

    print(f"Experiment Varying the the EMP states has Ended")
    configuration.setConfigParameters(defaultConfigParameters)    


def runDynamic_HintCarMap(configuration : Config , defaultConfigParameters : ConfigParameters , WorstCase = 0 ):

    """
        In this experiment we will see how the Dynamic State Varies if we increase the time gap between the mobility hint
        & Mobility Handover. In Sha Allah this will decrease the blocking time period
        Things to note :
            - for now this experiment will be performed for just the 4.1 % Dynamic State but we can add more variations
            - Experiment is for CarMap
            - Hint will start at 10 second time mark 
            - handover time will be fluctuated
    """
    hint = 10
    # handOvers = [10.005 , 10.010 , 10.050 , 10.075 , 10.1 , 10.2 , 10.5 , 11]
    handOvers = [10.05]
    totalVariables = 11
    dynamicVariables = 2
    UpdateRate = 1
    appName =  "CarMAP_APP" 
    print(f"Experiment Varying the CAR-MAP states has started : ")
    for handOver in handOvers :

        experimentConfigParameters = copy.deepcopy(defaultConfigParameters) 
        experimentConfigParameters.totalVariables = totalVariables
        experimentConfigParameters.dynamicVariables = dynamicVariables
        experimentConfigParameters.keySize = 10240 ## this doesnt matter though
        experimentConfigParameters.appName = appName
        experimentConfigParameters.hintTime = hint
        experimentConfigParameters.handoverTime = handOver
        experimentConfigParameters.WorstCase = WorstCase
        experimentConfigParameters.clientRequestRate = UpdateRate
        configuration.setConfigParameters(experimentConfigParameters)
        executeProcesses(experimentConfigParameters)


    print(f"Experiment Varying the CAR-MAP states has Ended")
    configuration.setConfigParameters(defaultConfigParameters) 


def runDynamic_StatesDefault2(configuration : Config , defaultConfigParameters : ConfigParameters , WorstCase = 0):

    """
        In this experiment we will work on varying the Dynamic State of the Process to observe how blocking Migration 
        is reduced over all. In Sha Allah we hope to see significant improvements due to preimptive migration & in built
        async redis migration 
        Things to note :
        -  mobility hint will start at 10 seconds mark
        -  handover will start at 10.1 aka after 100 ms
        -  App used will be the default edgeApp2 (can be checked from the common/keySaver.py)
        -  Total App will have 10 keys making the app state around 3.15 MB 
        -  The Update Rate will be of around 50 Updates/Second. We are aiming on mimicing highly updating application here
    """

    dynamicVariablesList = [2 , 4 , 6 , 8 , 10]
    totalVariables = 10
    hint = 10
    handover = 10.1
    appName = "EdgeCAT_APP2"
    keySize = (10*1024) ## Again this doesnt matter as keySaver.py will create Sizes by its own distribution
    UpdateRate = 50

    print(f"Experiment Varying the Default App states Variations has Started")

    for dynamicVariables in dynamicVariablesList :
        
        experimentConfigParameters = copy.deepcopy(defaultConfigParameters) 
        experimentConfigParameters.totalVariables = totalVariables
        experimentConfigParameters.dynamicVariables = dynamicVariables
        experimentConfigParameters.keySize = keySize ## this doesnt matter though
        experimentConfigParameters.appName = appName
        experimentConfigParameters.hintTime = hint
        experimentConfigParameters.handoverTime = handover
        experimentConfigParameters.clientRequestRate = UpdateRate
        experimentConfigParameters.WorstCase = WorstCase
        configuration.setConfigParameters(experimentConfigParameters)
        executeProcesses(experimentConfigParameters)

    print(f"Experiment Varying the Default App states Variations has Ended")


def runDynamic_StatesDefault1(configuration : Config , defaultConfigParameters : ConfigParameters , WorstCase = 0):

    """
        In this experiment we will work on varying the Dynamic State of the Process to observe how blocking Migration 
        is reduced over all. In Sha Allah we hope to see significant improvements due to preimptive migration & in built
        async redis migration 
        Things to note :
        -  mobility hint will start at 10 seconds mark
        -  handover will start at 10.1 aka after 100 ms
        -  App used will be the default edgeApp1 (can be checked from the common/keySaver.py)
        -  Total App will have 10 keys making the app state around 3 MB 
        -  The Update Rate will be of around 50 Updates/Second. We are aiming on mimicing highly updating application here
    """

    dynamicVariablesList = [1 , 2 , 3 , 4 , 5]
    totalVariables = 5
    hint = 10
    handover = 10.1
    appName = "EdgeCAT_APP"
    keySize = (1000*1024)
    UpdateRate = 50

    print(f"Experiment Varying the Default App states Variations has Started")

    for dynamicVariables in dynamicVariablesList :
        
        experimentConfigParameters = copy.deepcopy(defaultConfigParameters) 
        experimentConfigParameters.totalVariables = totalVariables
        experimentConfigParameters.dynamicVariables = dynamicVariables
        experimentConfigParameters.keySize = keySize ## this doesnt matter though
        experimentConfigParameters.appName = appName
        experimentConfigParameters.hintTime = hint
        experimentConfigParameters.handoverTime = handover
        experimentConfigParameters.clientRequestRate = UpdateRate
        experimentConfigParameters.WorstCase = WorstCase
        configuration.setConfigParameters(experimentConfigParameters)
        executeProcesses(experimentConfigParameters)

    print(f"Experiment Varying the Default App states Variations has Ended")



def main():

    # Register the SIGINT handler
    signal.signal(signal.SIGINT, signal_handler)

    ## Getting the Default Configuration , we will be changing the configuration for each experiment
    asyncMethods = [1]
    isDefaultMigrations = [1]
    keySizes = [128,512,1024,5120,10240,51200]
    

    ## Getting the Default Configuration Now :    
    configuration = Config("configuration/config.json")
    trueConfigParameters = configuration.GetConfigParameters()
    
    for j in range (0,100):

        for defaultMethod in isDefaultMigrations :

            print("Performing Experiments for the Default Migration Method : ", defaultMethod)
            defaultConfigParameters = copy.deepcopy(trueConfigParameters)
            defaultConfigParameters.isDefaultMigrationEnabled = defaultMethod

            if(defaultMethod == 1):
            

                ## UNCOMMENT THE FOLLOWING LINES FOR REPLICATING FIGURE 11 & 12
                runDynamic_HintCarMap(configuration , defaultConfigParameters , 0)
                # runDynamic_StatesDefault1(configuration , defaultConfigParameters , 0)

                ## UNCOMMENT THE FOLLOWING FOR TABLE 4 & 5 : GENERAL SCENARIO
                # runCarMap_Experiment(configuration , defaultConfigParameters , 0)
                # runEMP_Experiment(configuration , defaultConfigParameters , 0)                


                
            if (defaultMethod == 0):

                for asyncMethod in asyncMethods :

                    defaultConfigParameters.asyncType = asyncMethod
                    print("Performing Experiments for the Async Migration Method : ", defaultConfigParameters.asyncType)
                    
                    runDynamic_HintCarMap(configuration , defaultConfigParameters , 0)
                    # runDynamic_StatesDefault1(configuration , defaultConfigParameters , 0)

                    ## UNCOMMENT THE FOLLOWING FOR TABLE 4 & 5 : GENERAL SCENARIO
                    # runCarMap_Experiment(configuration , defaultConfigParameters , 0)
                    # runEMP_Experiment(configuration , defaultConfigParameters , 0)  


    print("Both scripts have finished executing.")
    configuration.setConfigParameters(trueConfigParameters)




if __name__ == "__main__":
    main()

