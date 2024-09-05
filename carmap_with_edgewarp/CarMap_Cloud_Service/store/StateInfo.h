#ifndef STATEINFO_H
#define STATEINFO_H

#include<iostream>
#include<chrono>
#include<string.h>


using namespace std;



#pragma once

class StateInfo
{
public:
    StateInfo(string newKey);
    ~StateInfo();

public: 
float GetFrequency();

public:
std::chrono::steady_clock::time_point updateTime;
std::chrono::steady_clock::time_point syncTime;
std::chrono::steady_clock::time_point creationTime;
string key;
uint32_t updateCounter;
uint32_t synCounter;
uint32_t keySize;
float updateFrequency;
};

#endif