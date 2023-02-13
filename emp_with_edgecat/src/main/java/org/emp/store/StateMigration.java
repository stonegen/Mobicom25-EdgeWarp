package org.emp.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.nd4j.linalg.primitives.Atomic;

import java.io.Console;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class StateMigration {
    private final String srcIp = "127.0.0.1";// todo: make it configurable
    private final int srcPort = 6379;// todo: make it configurable
    private final String destIp = "127.0.0.1";// todo: make it configurable (add your destination IP address here)
    private final int destPort = 6379;// todo: make it configurable
    final boolean isRedisStore = true; // will always be true for redisStore (memCache not supported at the moment)
    private final boolean isStateStoreRequired = true; // will always be true (we will always use store for this example)

    RedisStore stateStore;
    AtomicReference<StateTracker> stateTracker;
    StateReplication stateReplication;

    boolean isMobilityHintTriggered = false; // initially false (i.e. not triggered but will become true via logic one triggered)

    // Variables used for default migration only.
    boolean isDefaultMigrationTriggered = false; // initially false but will become true once blocking migration is triggered

    Thread replicationThread;

    long simulationTimeStart = System.currentTimeMillis();
    long simulationTimeNow = System.currentTimeMillis();
    long simulationDurationSeconds = Time.GetDurationSeconds(simulationTimeStart, simulationTimeNow);

    //    Configurable params
    // todo: make following params configurable
    final boolean IsDefaultMigrationModeSelected = false; // true: blocking, false: using mobility hint met
    final long MobilityHintToHandoverTimeDiff = 100; // in milliseconds. trigger mobility handover after 200 ms of hint (for testing)
    final int NoOfFramesToSync = 14; // 1, 5, 10 // no of recent frames to sync from store
    final int NoOfFramesInFuture = 2; // no of min latest frames to sync after mobility hint handover
    final int expTrigFrameId = 23; // 25, 50, 75 // there are total 96 frames. this value is the starting frameId from which we need to sync frames

    private List<String> keysToSync = new ArrayList<String>();

    public StateMigration() {
        Time.Init();

        System.out.println("State migration Mode Default = " + IsDefaultMigrationModeSelected);
        System.out.println("Mobility hint to handover time (ms) = " + MobilityHintToHandoverTimeDiff);

        StateTracker tracker = new StateTracker();
        stateTracker = new AtomicReference<StateTracker>(tracker);
        if (isRedisStore) {
            System.out.println("Redis store selected.");
            stateStore = new RedisStore(srcIp, srcPort, stateTracker, true);
            stateStore.Connect();
        } else
        {
            // Memcache store
        }

        stateReplication = new StateReplication(srcIp, srcPort, destIp, destPort, isRedisStore, stateTracker);
    }

    public void saveNewFrameChunk(String key, String value) throws InterruptedException {
        stateStore.Set(key, value);
    }

    public synchronized void Process(int latestFrameId) throws InterruptedException, ExecutionException {
        if (isStateStoreRequired)
        {
            simulationTimeNow = System.currentTimeMillis();
            simulationDurationSeconds = Time.GetDurationSeconds(simulationTimeStart, simulationTimeNow);

            if (IsDefaultMigrationModeSelected == false)
            {
                // Mobility hint event
                if (expTrigFrameId + NoOfFramesToSync == latestFrameId && isMobilityHintTriggered == false)
//                if (simulationDurationSeconds > MobilityHintTimeSeconds && isMobilityHintTriggered == false)
                {
                    // create a list of frame ids to sync
                    keysToSync = new ArrayList<String>();
                    int lastFrameToSync = latestFrameId + NoOfFramesInFuture;
                    int firstFrameToSync = lastFrameToSync - NoOfFramesToSync;
                    for (int fId = lastFrameToSync; fId >= 0 && fId > firstFrameToSync; fId--) {
                        keysToSync.add(String.valueOf("frame" + fId));
                    }
                    System.out.println("No of frames to sync = " + keysToSync.size());
                    System.out.println(Time.GetCurrentTime() + ": Mobility hint event triggerred.");
                    isMobilityHintTriggered = true;
                    // start new thread with syncing in the background
                    replicationThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            // code goes here.
                            try {
                                stateReplication.SyncBackground(keysToSync, MobilityHintToHandoverTimeDiff);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    replicationThread.start();
                }

                // Mobility handover event
//                if (simulationDurationSeconds > MobilityHandoverTimeSeconds && isMobilityHandoverTriggered == false)
//                {
//                    System.out.println(Time.GetCurrentTime() + ": Mobility handover event triggerred.");
//                    isMobilityHandoverTriggered = true;
//                    blockingMigrationTimeStart = System.currentTimeMillis();
//                    stateReplication.Notify();
//                    replicationThread.join();
//                    System.out.println(Time.GetCurrentTime() + ": Mobility handover completed.");
//                }
                // For now, we have moved Mobility Handover event inside the StateReplication class
            }
            else
            {
                // blocking handover
                if (expTrigFrameId + NoOfFramesToSync== latestFrameId && isDefaultMigrationTriggered == false)
//                if (simulationDurationSeconds > MobilityHandoverTimeSeconds && isDefaultMigrationTriggered == false)
                {
                    // create a list of frame ids to sync
                    keysToSync = new ArrayList<String>();
                    int lastFrameToSync = latestFrameId;
                    int firstFrameToSync = lastFrameToSync - NoOfFramesToSync;
                    for (int fId = lastFrameToSync; fId >= 0 && fId > firstFrameToSync; fId--) {
                        keysToSync.add(String.valueOf("frame" + fId));
                    }
                    System.out.println("No of frames to sync = " + keysToSync.size());

                    isDefaultMigrationTriggered = true;
                    // Now perform blocking migration with default scheme.

                    // Blocking migration of all the keys.
                    stateReplication.Migrate(keysToSync);

                }
            }
        }
    }
}
