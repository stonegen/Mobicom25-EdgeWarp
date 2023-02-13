#ifndef STATETRACKER_H
#define STATETRACKER_H

#pragma once

#include<iostream>
#include<string>
#include<vector>
#include<memory>
#include <mutex> 
#include<map>
#include"StateInfo.h"

using namespace std;

class StateTracker
{
public:
    StateTracker(shared_ptr<mutex> m_tx);
    ~StateTracker();

private:
const float UpdateRateUpperLimit = 10; 
const uint16_t KeySizeUpperLimit = 10*1024; // Don't sysc keys bigger than 10 KBs in advance.
std::chrono::steady_clock::time_point AppStartTime;
shared_ptr<mutex> mtx;

public:
map<string, shared_ptr<StateInfo>> StateStore;

public:
void AddKey(string key, uint32_t size);
void UpdateKey(string key);
void UpdateSyncTime(string key, std::chrono::steady_clock::time_point time);
vector<string> GetOldestUpdates();
vector<string> GetOutOfSyncKeys();
std::chrono::steady_clock::time_point GetLatestUpdateTime();
void PrintKeys();
// void AllSyncDone();
};

#endif