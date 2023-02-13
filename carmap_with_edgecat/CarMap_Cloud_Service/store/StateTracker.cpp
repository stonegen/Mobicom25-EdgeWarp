#include "StateTracker.h"
#include <memory>
#include <chrono>
#include <limits>

StateTracker::StateTracker(shared_ptr<mutex> m_tx)
{
    StateTracker::AppStartTime = std::chrono::steady_clock::now();
    mtx = m_tx;
}

StateTracker::~StateTracker()
{
}

void StateTracker::AddKey(string key, uint32_t size)
{
    shared_ptr<StateInfo> info = make_shared<StateInfo>(key);
    info->keySize = size;
    StateTracker::StateStore.insert(std::pair<string, shared_ptr<StateInfo>>(key, info));
}

void StateTracker::UpdateKey(string key)
{
    // If key exists
    if (StateStore.count(key) > 0)
    {
        StateStore[key]->updateTime = std::chrono::steady_clock::now();
    }
    else
    {
        shared_ptr<StateInfo> info = make_shared<StateInfo>(key);
        info->keySize = 0;
        StateTracker::StateStore.insert(std::pair<string, shared_ptr<StateInfo>>(key, info));
    }

    StateStore[key]->updateCounter = StateStore[key]->updateCounter + 1;
}

void StateTracker::UpdateSyncTime(string key, std::chrono::steady_clock::time_point time)
{
    if (StateStore.count(key) > 0)
    {
        StateStore[key]->syncTime = time;
        StateStore[key]->synCounter = StateStore[key]->synCounter + 1;
    }
}

vector<string> StateTracker::GetOldestUpdates()
{
    vector<string> keys;

    // Latest time.
    auto tempTime = std::chrono::steady_clock::now();

    for (auto item : StateStore)
    {
        if (item.second->keySize > KeySizeUpperLimit)
        {
            if (tempTime > item.second->updateTime && item.second->syncTime < item.second->updateTime)
            {
                tempTime = item.second->updateTime;
                keys.push_back(item.first);
            }
        }
        else
        {
            if (item.second->GetFrequency() <= UpdateRateUpperLimit)
            {
                if (tempTime > item.second->updateTime && item.second->syncTime < item.second->updateTime)
                {
                    tempTime = item.second->updateTime;
                    keys.push_back(item.first);
                }
            }
        }
    }
    

    return keys;
}

vector<string> StateTracker::GetOutOfSyncKeys()
{
    vector<string> keys;

    for (auto item : StateStore)
    {
        if (item.second->updateTime > item.second->syncTime)
        {
            keys.push_back(item.first);
        }
    }

    return keys;
}

std::chrono::steady_clock::time_point StateTracker::GetLatestUpdateTime()
{
    auto tempItem = StateStore.at(0);

    for (auto item : StateStore)
    {
        if (tempItem->updateTime < item.second->updateTime)
        {
            tempItem->updateTime = item.second->updateTime;
        }
    }

    return tempItem->updateTime;
}

void StateTracker::PrintKeys()
{
    for (auto item : StateStore)
    {
        cout << "Key = " << item.first << ", Size = " << item.second->keySize << ", Update Counter = " << item.second->updateCounter << ", Update Frequency = " << item.second->updateFrequency << ", Sync Counter = " << item.second->synCounter << endl;
    }
}
// void StateTracker::AllSyncDone();