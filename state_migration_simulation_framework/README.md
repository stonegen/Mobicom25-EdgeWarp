## Explaination for the Simulation FrameWork :

### Basic flow of the simulation :

The Simulation framework operates by running the **appserver.py** which will create a server instance listening on the provided IP and Port number from the clients for the normal messages and **mobility.py** for the **mobility hint & handover** related messages. 

Secondly, when launching the appserver.py an instance of **mobilityHandler.py** will be created which is in gist, just a listener for the messages for the mobility related messages i.e. **mobility hint & handover**. These messages will be triggered by the client which the mobility handler will listen and push these messages over to the queue that **server instance** from appserver.py will be process appropriately.

Lastly, the **appclient.py** initiates a client version that basically just messages over to the server and mobility handler based on the configuration provided. 

Every instance is directly or indirectly related to the **configuration** provided to the framework. *(Explained in detail in the next section)*


### Configuration _(The brain of the Simulation)_ :

**Path** : state_migration_simulation_framework/configuration

**OverView** :
The core of the simulation resides in the **config.json** which has multiple properties that defines how each component of the simulation has to operate.
The files such as **configParameters.py** & **config.py** are mainly responsible for the parsing the config.json and initiate a configuration class instance that could be used in the rest of the python files for to extract the parameters. Additionally, this also allows to change config.json (used mainly in the **startEvaluation.py**).

**Config.json** :
Below is the explaination for each of the parameter involved in the configuration :

- localp : this will be the IP address on which **server** and source **Redis/Memcache** instance will be operating. (Recall that in the design architecture both DataStore (source server) and Edge Server are operating on the single machine) 

- portNumber : portnumber for the server to listen to

- mobilityPort : portnumber for the mobilityPort to listen to

- remoteIP : Ip address for the destination DataStore server. This will be the datastore towards which our client is    migrating into

- storePassword : (unnecessary detail that will be removed in future updates)

- storePortNumber : The port on which source DataStore will be having the client's state. This state will then be migrated towards the destination

- dynamicVariables : So basically an edge Application has n keys . From this n keys there will x keys such that 0 < x <= n and these keys will be updated after specific configured time interval. 

- totalVariables : the total number of the keys involved in the edge app

- keySize : the keySize of a key involved in the Edge App

- isDefaultMigrationEnabled : this takes 2 values 0 or 1 . _*(to learn about the new asynchronous migration scheme please read into redis-unstable/readMe file )*_
    - 0 means : we will be using new asynchronous migration method 
    - 1 means : we will be using the synchronous migration method 

- asyncType : please ignore this and keep the value to 1. 

- storeType : "Redis" or "MemCache" . Basically this specifies the type of the dataStore involved 

- hintTime : This will be the exact time in seconds upon which the simulation client will trigger a mobility hint message to the hand mobilityHandler instance

- handOverTime : Like hintTime in seconds, client will now send a handOver related message to mobilityHandler instance. **However, handOverTime > hintTime** 

- numberOfClients : clients in the simulation. Please keep this number to 1 for now

- clientUpdateRate : the frquency a.k.a the number of times the client will send message to the edge server in a second

- minOldestUpdates & maxOldestUpdates : Basically when the mobility hint is recieved, the Edge Server will select inbetween atleast minOldest (minimum number of oldest keys) and atmost maxOldest (maximum number of the oldest keys) to migrate from source DataStore to Destination DataStore. However once handOver is occured, all of the keys will be migrated that are updated

- stateMethod : two schemes are created in the simulation for the state migration once hint is recieved. 
    - LFU : select the least frequently used keys to migrate
    - LRU : select the least recently used keys to migrate

- appName : there are multiple types of the apps created for the simulation. Please look into the **keySaver.py**

- WorstCase : This was scenario created for testing the performance of asynchronous migration under the worst case i.e. all of the keys are updated just before HandOver is recieved.



### SERVER _(the heart of the simulation)_ :

Given the configuration above the **appServer.py** first initates a mobiliy handler instance and waits for the client to connect. Once a client connected, it listen for it's messages and replies to them appropriately. The client from **client.py** sends the message based on the interval decided by the configuration and waits for it's replies. 

The server based on the configuration initialises the thread that will be performing the asynchronous migration on the configured dataStore. Before mobility hint, every message created by the client will result into the states updation of all of the dynamic variables in the edge app.

Normal DAG message flow from between server and client :

- Normal messages (these will be send till handOver is occured) : Client &rarr; Server
- Hint and HandOver messages : Client &rarr; Mobility Handler &rarr; Server &rarr; BackGroundThreads for the Asynchronous Migration (if default Migration is 0)


Once the hint is recieved to the server, given the default migration scheme = 0, the background thread will be initialised from the code **state_migration_simulation_framework/common/redisStore.py**. This will start an asynchronous server of it's own to interact with the Redis Source & Destination Server to send over the states asynchronously. 

Once the handOver is recieved , the background thread involved in the asynchronous migration will be closed while simulatenously another thread for the handOver will be launched to start the handOver from **state_migration_simulation_framework/common/redisStore.py**. 

After the state is migrated completely, the socket connection with the client is closed gracefully the scripts are exited.



