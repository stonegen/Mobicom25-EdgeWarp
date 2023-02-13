#ifndef REDISSTORE_H
#define REDISSTORE_H

#pragma once

#include "Store.h"
#include "StateTracker.h"
#include <cpp_redis/cpp_redis>
#include <mutex>

class RedisStore: public Store
{
public:
    RedisStore(string ipaddress, int portNumber, shared_ptr<StateTracker> tracker, bool sourceStore, shared_ptr<mutex> m_tx);
    ~RedisStore();

public:
    void Connect();
    void Disconnect();
    bool Set(string key, string data);
    string Get(string key);
    // void Migrate(string destinationIp, std::vector<string> keys);


private:
    string ip;
    int port;
    bool isSourceStore;

private:
    cpp_redis::client redisClient;
    shared_ptr<StateTracker> stateTracker;
    shared_ptr<mutex> mtx;

};

#endif