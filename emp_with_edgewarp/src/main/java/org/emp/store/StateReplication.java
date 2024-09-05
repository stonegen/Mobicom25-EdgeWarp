package org.emp.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StateReplication {

    String sourceIpAddress;
    int sourcePortNumber;
    String destinationIpAddress;
    int destinationPortNumber;
    boolean isRedisStoreSelected;

    RedisStore sourceStore;
    RedisStore destinationStore;
    AtomicReference<StateTracker> stateTracker;

    // Variables for event generations

    AtomicBoolean ready = new AtomicBoolean(false);
    Lock lock = new ReentrantLock();
    Condition cv = lock.newCondition();

    StateReplication(String sourceIp, int sourcePort, String destinationIp, int destinationPort,
                     boolean isRedisStore, AtomicReference<StateTracker> tracker) {
        sourceIpAddress = sourceIp;
        sourcePortNumber = sourcePort;
        destinationIpAddress = destinationIp;
        destinationPortNumber = destinationPort;
        isRedisStoreSelected = isRedisStore;
        stateTracker = tracker;

        if (isRedisStore)
        {
            sourceStore = new RedisStore(sourceIp, sourcePort, tracker, false);
            sourceStore.Connect();

            destinationStore = new RedisStore(destinationIp, destinationPort, tracker, false);
            destinationStore.Connect();
        }
    }

    public long PerformAction(long totalSize, String key, long syncTimeBegin, List<String> outOfSyncKeys) throws ExecutionException, InterruptedException {
        if (sourceStore.HasKey(key)) {
            System.out.println(Time.GetCurrentTime() + ": Syncing key = " + key);
            // update only if key has the data
            String data = sourceStore.Get(key);
            totalSize += data.length();
            boolean status = destinationStore.Set(key, data);

            // If key is stored on destination store
            if (status)
            {
                StateTracker updatedTracker = stateTracker.get();
                updatedTracker.UpdateSyncTime(key, syncTimeBegin);
                stateTracker.set(updatedTracker);
            }
            // remove the keys that have been synced
            outOfSyncKeys.remove(key);
        }
        return totalSize;
    }
    public synchronized void SyncBackground(List<String> keys, long mobilityHintToHandoverTimeDiff) throws InterruptedException, ExecutionException {
        boolean keepSynching = true;
        System.out.println(Time.GetCurrentTime() + ": Modified scheme: Background sync process started.");

        long totalSize = 0;

        List<String> outOfSyncKeys = new ArrayList<String>(keys);
        final long mobilityHintTriggeredTime = System.currentTimeMillis();
        System.out.println(Time.GetCurrentTime() + ": out of sync keys: " + outOfSyncKeys);
        System.out.println(Time.GetCurrentTime() + ": Non-Blocking migration keys count: " + outOfSyncKeys.size());
        while (keepSynching)
        {
            long syncTimeBegin = System.currentTimeMillis();

            final long min = 0;
            final long max = outOfSyncKeys.size() - 1;
            if (outOfSyncKeys.size() > 0) {
                int keyIndex = (int)(Math.random() * (max - min + 1) + min); // choose next key to sync at random
                String key = outOfSyncKeys.get(keyIndex);

                totalSize = PerformAction(totalSize, key, syncTimeBegin, outOfSyncKeys);
            }

            // Mobility handover signal received (simulated).
            if (System.currentTimeMillis() >= mobilityHintTriggeredTime + mobilityHintToHandoverTimeDiff)
            {
                System.out.println(System.currentTimeMillis() + " >= " + mobilityHintTriggeredTime + " + " + mobilityHintToHandoverTimeDiff);
                System.out.println(Time.GetCurrentTime() + ": Mobility handover event triggerred.");
                System.out.println(Time.GetCurrentTime() + ": Blocking migration signal received. ");
                System.out.println(Time.GetCurrentTime() + ": out of sync keys: " + outOfSyncKeys);

                syncTimeBegin = System.currentTimeMillis();
                System.out.println(Time.GetCurrentTime() + ": Blocking migration keys count: " + outOfSyncKeys.size());

                for (String key : outOfSyncKeys)
                {
                    totalSize = PerformAction(totalSize, key, syncTimeBegin, outOfSyncKeys);
                }
//                sourceStore.Connect();
                long syncTimeEnd = System.currentTimeMillis();
                System.out.println(Time.GetCurrentTime() + ": Modified Scheme: Blocking migration time = " + Time.GetDurationMs(syncTimeBegin, syncTimeEnd) + " (ms)");
                keepSynching = false;

                System.out.println(Time.GetCurrentTime() + ": Mobility handover completed.");
                sourceStore.Disconnect();

                System.out.println("TOTAL: " + totalSize);
            }

//            try {
//                lock.lock();
//                cv.await(1, TimeUnit.MICROSECONDS);
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//            }
//
//            try {
//                if (ready.get())
//                {
//                    System.out.println(Time.GetCurrentTime() + ": Blocking migration signal received. ");
//                    System.out.println(outOfSyncKeys);
//
//                    outOfSyncKeys = stateTracker.get().GetOutOfSyncKeys();
//                    syncTimeBegin = System.currentTimeMillis();
//
//                    System.out.println(Time.GetCurrentTime() + ": Blocking migration keys count: " + outOfSyncKeys.size());
//
//                    for (String key : outOfSyncKeys)
//                    {
//                        String data = sourceStore.Get(key);
//                        boolean status = destinationStore.Set(key, data);
//
//                        // If key is stored on destination store
//                        if (status)
//                        {
//                            StateTracker updatedTracker = stateTracker.get();
//                            updatedTracker.UpdateSyncTime(key, syncTimeBegin);
//                            stateTracker.set(updatedTracker);
//                        }
//                    }
//
//                    sourceStore.Disconnect();
////                sourceStore.Connect();
//                    long syncTimeEnd = System.currentTimeMillis();
//                    System.out.println(Time.GetCurrentTime() + ": Modified Scheme: Blocking migration time = " + Time.GetDurationMs(syncTimeBegin, syncTimeEnd) + " (ms)");
//                    keepSynching = false;
//                    return;
//                }
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//            }

        }
    }

    public synchronized void Notify() {
        ready.set(true);
        cv.notify();
    }

    public void Migrate(List<String> keys)throws InterruptedException, ExecutionException {
        System.out.println("Default migration starting. ");
        long syncTimeBegin = System.currentTimeMillis();

        for (String key : keys)
        {
            if (sourceStore.HasKey(key)) {
                System.out.println(Time.GetCurrentTime() + ": -->> Syncing key = " + key);
                String data = sourceStore.Get(key);
                boolean status = destinationStore.Set(key, data);

                // If key is stored on destination store
                if (status) {
                    StateTracker updatedTracker = stateTracker.get();
                    updatedTracker.UpdateSyncTime(key, syncTimeBegin);
                    stateTracker.set(updatedTracker);
                }
            }
        }
        long syncTimeEnd = System.currentTimeMillis();
        System.out.println("Default Scheme: Blocking migration time = " + Time.GetDurationMs(syncTimeBegin, syncTimeEnd) + " (ms)");
        sourceStore.Disconnect();
//        sourceStore.Connect();
    }
}
