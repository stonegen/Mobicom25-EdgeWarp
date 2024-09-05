#ifndef STATEREPLICATION_H
#define STATEREPLICATION_H

#pragma once

#include <string>
#include <iostream>
#include <memory>
#include <atomic>
#include<mutex>
#include <condition_variable>
#include "Store.h"
#include "RedisStore.h"

using namespace std;

class StateReplication
{
public:
    StateReplication(string sourceIp, int sourcePort, string destinationIp, int destinationPort, 
    bool isRedisStore, shared_ptr<StateTracker> tracker, shared_ptr<mutex> m_tx);
    ~StateReplication();

public:
void SyncBackground(string callingThread);
void Migrate(std::vector<string> keys);
void Notify();

private:
string sourceIpAddress;
int sourcePortNumber;
string destinationIpAddress;
int destinationPortNumber;
bool isRedisStoreSelected;

shared_ptr<Store> sourceStore;
shared_ptr<Store> destinationStore;
shared_ptr<StateTracker> stateTracker;
shared_ptr<mutex> mtx;

// Variables for event generations
private:
std::atomic<bool> ready;
std::mutex lock;
std::condition_variable cv;
};

#endif