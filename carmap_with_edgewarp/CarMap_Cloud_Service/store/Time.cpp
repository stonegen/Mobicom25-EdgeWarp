#include "Time.h"

Time::Time()
{

}

Time::~Time()
{

}

std::chrono::steady_clock::time_point Time::startTime = std::chrono::steady_clock::now();

void Time::Init()
{
    Time::startTime = std::chrono::steady_clock::now();
}

double Time::GetDuratioinMs(std::chrono::steady_clock::time_point startTime, std::chrono::steady_clock::time_point endTime)
{
    auto durationMs = (std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count());
    return durationMs;
}

double Time::GetCurrentTime()
{
    auto endTime = std::chrono::steady_clock::now();
    auto durationMs = (std::chrono::duration_cast<std::chrono::microseconds>(endTime - Time::startTime).count()) / 1000.0f;
    return durationMs;
}