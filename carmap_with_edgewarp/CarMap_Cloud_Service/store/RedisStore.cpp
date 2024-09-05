#include <iostream>
#include "RedisStore.h"
#include "StateTracker.h"

using namespace std;

RedisStore::RedisStore(string ipaddress, int portNumber, shared_ptr<StateTracker> tracker, bool sourceStore, shared_ptr<mutex> m_tx)
{
    RedisStore::ip = ipaddress;
    RedisStore::port = portNumber;
    RedisStore::stateTracker = tracker;
    RedisStore::isSourceStore = sourceStore;
    RedisStore::mtx = m_tx;
}

RedisStore::~RedisStore()
{
    redisClient.disconnect();
    cout << "Client disconnected" << endl;
}

void RedisStore::Connect()
{
    if (redisClient.is_connected())
    {
        redisClient.disconnect();
    }

    cout << "Connecting to Redis store on IP: " << RedisStore::ip << " Port:" << RedisStore::port << endl;
    RedisStore::redisClient.connect(RedisStore::ip, RedisStore::port);
    RedisStore::redisClient.auth("testpass");
    cout << "Connected to Redis store." << endl;
}

void RedisStore::Disconnect()
{
    RedisStore::redisClient.disconnect();
}

bool RedisStore::Set(string key, string data)
{
    bool saveSuccessful = false;
    redisClient.set(key, data, [&saveSuccessful](cpp_redis::reply &reply)
                    {
    if (reply.as_string().find("OK") != std::string::npos)
    {
        saveSuccessful = true;
        // cout << "**************** Status : " << saveSuccessful << endl;
    } });

    redisClient.sync_commit(std::chrono::milliseconds(10000));

    if (isSourceStore && saveSuccessful)
    {
        mtx->lock();
        stateTracker->UpdateKey(key);
        mtx->unlock();
    }

    return saveSuccessful;
}

string RedisStore::Get(string key)
{
    std::string response = "";
    redisClient.get(key, [&response](cpp_redis::reply &reply)
                    {
    if (reply.is_string())
    {
        response = reply.as_string();
    } });

    redisClient.sync_commit(std::chrono::milliseconds(100));

    return response;
}

// void RedisStore::Migrate(string destinationIp, std::vector<string> keys)
// {
//     cpp_redis::client redisDestinationClient;
//     redisDestinationClient.connect(destinationIp, RedisStore::port);
//     redisDestinationClient.auth("testpass");
//     cout << "Migration started to IP: " << destinationIp << endl;

//     // Migrating all the keys to the destination machine.
//     for (string key : keys)
//     {
//         // cout << "Migrating key: " << key << endl;
//         auto sourceValue = Get(key);
//         redisDestinationClient.set(key, sourceValue, [](cpp_redis::reply &reply){});
//         redisDestinationClient.sync_commit(std::chrono::milliseconds(100));

//         // RedisStore::redisClient.migrate(RedisStore::ip, RedisStore::port, key, "0", 2000, true, true);
//     }
//     // RedisStore::redisClient.migrate(RedisStore::ip, RedisStore::port, "", "0", 2000, true, true, keys, {});
//     // cout << "Migration finished!" << endl;
// }
