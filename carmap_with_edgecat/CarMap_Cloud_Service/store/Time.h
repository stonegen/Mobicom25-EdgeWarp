#ifndef TIME_H
#define TIME_H

#pragma once
#include <chrono>

class Time
{
public:
    Time();
    ~Time();

public:
static void Init();
static double GetDuratioinMs(std::chrono::steady_clock::time_point startTime, std::chrono::steady_clock::time_point endTime);
static double GetCurrentTime();

public:
static std::chrono::steady_clock::time_point startTime;

};

#endif