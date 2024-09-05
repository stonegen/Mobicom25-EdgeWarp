#include "StateReplication.h"
#include "Time.h"

StateReplication::StateReplication(string sourceIp, int sourcePort, string destinationIp,
                                   int destinationPort, bool isRedisStore, shared_ptr<StateTracker> tracker, shared_ptr<mutex> m_tx)
{
    sourceIpAddress = sourceIp;
    sourcePortNumber = sourcePort;
    destinationIpAddress = destinationIp;
    destinationPortNumber = destinationPort;
    isRedisStoreSelected = isRedisStore;
    mtx = m_tx;

    stateTracker = tracker;

    if (isRedisStore)
    {
        sourceStore = make_shared<RedisStore>(sourceIp, sourcePort, tracker, false, mtx);
        sourceStore->Connect();

        destinationStore = make_shared<RedisStore>(destinationIp, destinationPort, tracker, false, mtx);
        destinationStore->Connect();
    }
}

StateReplication::~StateReplication()
{
}

void StateReplication::SyncBackground(string callingThread)
{
    auto keepSynching = true;
    cout << Time::GetCurrentTime() << ": Modified scheme: Background sync process started."<< endl;

    while (keepSynching)
    {
        auto syncTimeBegin = std::chrono::steady_clock::now();
        // auto outOfSyncKeys = stateTracker->GetOldestUpdates();

        mtx->lock();
        auto outOfSyncKeys = stateTracker->GetOutOfSyncKeys();
        mtx->unlock();

        for (auto key : outOfSyncKeys)
        {
            auto data = sourceStore->Get(key);
            bool status = destinationStore->Set(key, data);

            // If key is stored on desination store
            if (status)
            {
                mtx->lock();
                stateTracker->UpdateSyncTime(key, syncTimeBegin);
                mtx->unlock();
            }
        }

        // Mobility handover signal received.
        std::unique_lock<std::mutex> stackLock(lock);
        cv.wait_for(stackLock, std::chrono::microseconds(1));
        if (ready)
        {
            cout << Time::GetCurrentTime() << ": Blocking migrtion signal received. "<< endl;
            auto simulationTimeStart = std::chrono::steady_clock::now();
            // cout << Time::GetCurrentTime() << ": Going to start blocking synchronization here." << endl;

            mtx->lock();
            auto outOfSyncKeys = stateTracker->GetOldestUpdates();
            mtx->unlock();

            auto syncTimeBegin = std::chrono::steady_clock::now();

            cout << Time::GetCurrentTime() << ": Blocking migration keys count: " << outOfSyncKeys.size() << endl;

            for (auto key : outOfSyncKeys)
            {
                auto data = sourceStore->Get(key);
                bool status = destinationStore->Set(key, data);

                // If key is stored on desination store
                if (status)
                {
                    mtx->lock();
                    stateTracker->UpdateSyncTime(key, syncTimeBegin);
                    mtx->unlock();
                }
            }

            sourceStore->Disconnect();
            sourceStore->Connect();
            auto syncTimeEnd = std::chrono::steady_clock::now();
            cout << Time::GetCurrentTime() << ": Modified Scheme: Blocking migration time = " << Time::GetDuratioinMs(syncTimeBegin, syncTimeEnd) << " (ms)" << endl;
            keepSynching = false;
            return;
        }
    }
}

void StateReplication::Notify()
{
    StateReplication::ready = true;
    StateReplication::cv.notify_one();
}

void StateReplication::Migrate(std::vector<string> keys)
{
    cout << "Default migration starting. " << endl;
    auto syncTimeBegin = std::chrono::steady_clock::now();
    for (auto key : keys)
    {
        auto data = sourceStore->Get(key);
        bool status = destinationStore->Set(key, data);

        // If key is stored on desination store
        if (status)
        {
            mtx->lock();
            stateTracker->UpdateSyncTime(key, syncTimeBegin);
            mtx->unlock();
        }
    }
    sourceStore->Disconnect();
    sourceStore->Connect();
    auto syncTimeEnd = std::chrono::steady_clock::now();
    cout << "Default Scheme: Blocking migration time = " << Time::GetDuratioinMs(syncTimeBegin, syncTimeEnd) << " (ms)" << endl;
}