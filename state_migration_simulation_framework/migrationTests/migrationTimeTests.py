import threading
import select
import socket
import time
import redis
import csv
from multiprocessing import Process,Event


# Define the IP address and ports for Redis servers
REDIS_SOURCE_HOST = '127.0.0.1'
REDIS_SOURCE_PORT = 6378
REDIS_DESTINATION_HOST = '127.0.0.1'
REDIS_DESTINATION_PORT = 6384
CLIENT_RECIEVER_HOST = '127.0.0.1'
CLIENT_RECIEVER_PORT = 6386
TOTAL_KEYS = 100
KEYS_TO_MIGRATE = 1
KEY_SIZE = 10240
KEY_PREFIX = "Kib"
KEY_VALUE = 'b' * (KEY_SIZE)

stopAsyncListening = Event()
eventAsyncReplyRecieved = Event()



def startListening(receiverHost, receiverPort):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((receiverHost, receiverPort))
    server_socket.listen()
    # server_socket.setblocking(False)  # Optional: If you want non-blocking mode

    print(f"Listening on {receiverHost}:{receiverPort}...")

    try:
        # Continuously listen for asynchronous replies until a mobility hint is offered
        while not stopAsyncListening.is_set():
            print("Waiting for a new connection...")
            conn, addr = server_socket.accept()
            print("NEW CONNECTION CREATED!")
            with conn:
                conn.settimeout(2)
                while not stopAsyncListening.is_set():
                    try :
                        data = conn.recv(1024)
                        if data:
                            strData = data.decode('utf-8')
                            print("Message received:", strData)
                            if strData == "MIGRATION COMPLETE!":
                                eventAsyncReplyRecieved.set()
                            else:
                                eventAsyncReplyRecieved.set()  # Assuming other messages also set the event
                        data = None
                    except socket.timeout :
                                continue

            conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        server_socket.close()
        print("Server socket closed.")

        
## STEP 1 : We will populate the Redis Data Store with N keys with M Size :

def populate_redis_source():
    client = redis.Redis(host=REDIS_SOURCE_HOST, port=REDIS_SOURCE_PORT)
    value = KEY_VALUE
    
    for i in range(TOTAL_KEYS):
        response = client.set(f'{KEY_PREFIX}{i}', value)
        if not response:
            print(f"Failed to set key{i}")
    print("Finished setting 1000 keys in the source Redis server")


# Function to log GET request times in a separate thread from destination Server 
def log_get_requests( host , port ):
    client = redis.Redis(host,port)
    for i in range(TOTAL_KEYS):
        response = client.get(f'{KEY_PREFIX}{i}')
        response2 = client.set(f'{KEY_PREFIX}_{i}', KEY_VALUE)
        time.sleep(0.01)
        if response.decode('utf-8') != KEY_VALUE :
            print(f"TEST CASE FAILED : WE DIDN'T GET THE KEY : {KEY_PREFIX}{i} FROM DESTINATION SERVER")


# Function to send the migration command
def send_migration_command():

    with open('migration_test_log2.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Time Taken (ms)', 'Operation'])
        client = redis.Redis(host=REDIS_SOURCE_HOST, port=REDIS_SOURCE_PORT)
        client2 = redis.Redis(host=REDIS_DESTINATION_HOST, port=REDIS_DESTINATION_PORT)

        listen_thread = Process(target=startListening , args= [CLIENT_RECIEVER_HOST , CLIENT_RECIEVER_PORT])
        listen_thread.start()
        print("Hello!")
        startIndex = 0
        while startIndex < TOTAL_KEYS :

            keyStr = ' '.join([f'{KEY_PREFIX}{i}' for i in range(startIndex , (startIndex + KEYS_TO_MIGRATE))])
            migration_command = f'MIGRATE ASYNC {REDIS_DESTINATION_HOST} {REDIS_DESTINATION_PORT} {CLIENT_RECIEVER_HOST} {CLIENT_RECIEVER_PORT} "" 0 50000000 REPLACE KEYS ' + keyStr
            t1 = time.time()
            client.execute_command(migration_command)  
            print("yesss!")
            eventAsyncReplyRecieved.wait()
            t2 = time.time()
            csv_writer.writerow([(t2-t1)*1000, 'MIGRATION_TIME'])
            eventAsyncReplyRecieved.clear()
            startIndex = startIndex + KEYS_TO_MIGRATE
            startI = 0
            

            time.sleep(0.02)
            
        stopAsyncListening.set()
        listen_thread.join()

        print("Closing the connection !")
        migration_command = f'MIGRATE CLOSE {CLIENT_RECIEVER_HOST} {CLIENT_RECIEVER_PORT} {REDIS_DESTINATION_HOST} {REDIS_DESTINATION_PORT}'
        migrateResponse = client.execute_command(migration_command)     
        migrateResponse = client2.execute_command(migration_command)           
        print("Migrate close response is : ", migrateResponse)




# Main function to coordinate the test
def main():
    ## Populating the Source Redis Server :
    populate_redis_source()

    # ## starting the Migration:
    listen_thread = Process(target=send_migration_command)
    # send_migration_command()
    # get_thread = Process(target=  log_get_requests(REDIS_SOURCE_HOST , REDIS_SOURCE_PORT))
    listen_thread.start()
    listen_thread.join()
    # get_thread.start()
    # get_thread.join()

    ## Ensuring All keys are saved into destination or not ?

    # log_get_requests(REDIS_DESTINATION_HOST , REDIS_DESTINATION_PORT)
    print("Get LOGS completed !")
        


    
if __name__ == "__main__":
    main()
















































