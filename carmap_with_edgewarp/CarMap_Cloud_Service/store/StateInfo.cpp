#include "StateInfo.h"

StateInfo::StateInfo(string newKey)
{
    updateTime = std::chrono::steady_clock::now ();
    syncTime = std::chrono::steady_clock::now ();
    creationTime = std::chrono::steady_clock::now ();
    key = newKey;
    updateCounter = 0;
    synCounter = 0;
    keySize = 0;
    updateFrequency = 0;
}

StateInfo::~StateInfo()
{

}

float StateInfo::GetFrequency()
{
    float frequency = 0;

    // Wait for a few updates before getting frequency
    if (updateCounter >= 3)
    {
         auto timeNow = std::chrono::steady_clock::now ();
        frequency = (std::chrono::duration_cast<std::chrono::microseconds>(timeNow - creationTime).count())/updateCounter;
    }

    return frequency;
}