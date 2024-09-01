import asyncio
import socket
import time
import redis
import csv
from multiprocessing import Process, Event

# Define the IP address and ports for Redis servers
REDIS_SOURCE_HOST = '127.0.0.1'
REDIS_SOURCE_PORT = 6378
REDIS_DESTINATION_HOST = '127.0.0.1'
REDIS_DESTINATION_PORT = 6384
CLIENT_RECEIVER_HOST = '127.0.0.1'
CLIENT_RECEIVER_PORT = 6386
TOTAL_KEYS = 100
KEYS_TO_MIGRATE = 1
KEY_SIZE = 10240
KEY_PREFIX = "Kib"
KEY_VALUE = 'b' * (KEY_SIZE)

STOP_DUMMY_EVENT = Event()

def do_Dummy_Gets():

    while not STOP_DUMMY_EVENT :

        client = redis.Redis(host=REDIS_SOURCE_HOST, port=REDIS_SOURCE_PORT)
        client.get(f'{KEY_PREFIX}1')

        time.sleep(1)

        



async def handle_client(reader, writer, reply_event):
    while True:
        data = await reader.read(1024)
        if data:
            str_data = data.decode('utf-8')
            print("Message received:", str_data)
            if str_data == "MIGRATION COMPLETE!":
                reply_event.set()
                continue

async def start_listening(receiver_host, receiver_port, reply_event):
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, reply_event), 
        receiver_host, receiver_port
    )
    async with server:
        await server.serve_forever()

async def populate_redis_source():
    client = redis.Redis(host=REDIS_SOURCE_HOST, port=REDIS_SOURCE_PORT)
    value = KEY_VALUE
    for i in range(TOTAL_KEYS):
        response = client.set(f'{KEY_PREFIX}{i}', value)
        if not response:
            print(f"Failed to set key{i}")
    print("Finished setting 1000 keys in the source Redis server")

async def send_migration_command():
    client = redis.Redis(host=REDIS_SOURCE_HOST, port=REDIS_SOURCE_PORT)
    client2 = redis.Redis(host=REDIS_DESTINATION_HOST, port=REDIS_DESTINATION_PORT)
    reply_event = asyncio.Event()

    # Start the listener
    listen_task = asyncio.create_task(start_listening(CLIENT_RECEIVER_HOST, CLIENT_RECEIVER_PORT, reply_event))
    
    with open('migration_test_log2.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Time Taken (ms)', 'Operation'])

        start_index = 0
        while start_index < TOTAL_KEYS:
            key_str = ' '.join([f'{KEY_PREFIX}{i}' for i in range(start_index, start_index + KEYS_TO_MIGRATE)])
            migration_command = f'MIGRATE ASYNC {REDIS_DESTINATION_HOST} {REDIS_DESTINATION_PORT} {CLIENT_RECEIVER_HOST} {CLIENT_RECEIVER_PORT} "" 0 50000000 REPLACE KEYS {key_str}'

            t1 = time.time()
            client.execute_command(migration_command)
            print("Migration command sent!")

            # Wait for the listener to signal that the message has been received
            await reply_event.wait()
            t2 = time.time()

            print((t2-t1) * 1000)
            # csv_writer.writerow([(t2 - t1) * 1000, 'MIGRATION_TIME'])

            reply_event.clear()  # Reset the event for the next iteration

            start_index += KEYS_TO_MIGRATE

            i = 0
            while (i < 1000000):
                i = i +1 

        print("Closing the connection!")
        migration_command = f'MIGRATE CLOSE {CLIENT_RECEIVER_HOST} {CLIENT_RECEIVER_PORT} {REDIS_DESTINATION_HOST} {REDIS_DESTINATION_PORT}'
        migrate_response = client.execute_command(migration_command)
        migrateResponse = client2.execute_command(migration_command)
        print("Migrate close response is:", migrate_response)

        
    # Stop the listener
    listen_task.cancel()

async def main():
    await populate_redis_source()

    ## Launching Multi Process :
    # myProcess = Process(target=do_Dummy_Gets)
    # myProcess.start()
    await send_migration_command()

    STOP_DUMMY_EVENT.set()
    # myProcess.join()
    STOP_DUMMY_EVENT.clear()


    print("Get LOGS completed!")


def mainMain() :
    asyncio.run(main())


if __name__ == "__main__":

    mainP = Process(target=mainMain)
    mainP.start()
    mainP.join()

