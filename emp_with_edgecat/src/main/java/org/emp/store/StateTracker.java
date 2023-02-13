package org.emp.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StateTracker {
    final float UpdateRateUpperLimit = 10; // todo: make it configurable
    // Don't sync keys bigger than 10 KBs in advance.
    final long KeySizeUpperLimit = 10*1024; // todo: make it configurable
    long AppStartTime;

    Map<String, StateInfo> StateStore = new HashMap<String, StateInfo>();

    StateTracker()
    {
        AppStartTime = System.currentTimeMillis();
    }

    public void AddKey(String key, long size)
    {
        StateInfo info = new StateInfo(key);
        info.setKeySize(size);
        StateStore.put(key, info);
    }

    public void UpdateKey(String key)
    {
        // If key exists
        if (StateStore.containsKey(key))
        {
            StateStore.get(key).setUpdateTime(System.currentTimeMillis());
        }
        else
        {
            StateInfo info = new StateInfo(key);
            info.setKeySize(0);
            StateStore.put(key, info);
        }
        StateStore.get(key).setUpdateCounter(StateStore.get(key).getUpdateCounter() + 1);
    }

    public void UpdateSyncTime(String key, long time)
    {
        if (StateStore.containsKey(key))
        {
            StateStore.get(key).setSyncTime(time);
            StateStore.get(key).setSynCounter(StateStore.get(key).getSynCounter() + 1);
        }
    }

    public List<String> GetOldestUpdates()
    {
        List<String> keys = new ArrayList<String>();

        // Latest time.
        long tempTime = System.currentTimeMillis();

        for (Map.Entry<String,StateInfo> item : StateStore.entrySet())
        {
            String key = item.getKey();
            StateInfo value = item.getValue();
            if (value.getKeySize() > KeySizeUpperLimit)
            {
                if (tempTime > value.getUpdateTime() && value.getSyncTime() < value.getUpdateTime()) {
                    tempTime = value.getUpdateTime();
                    keys.add(key);
                }
            } else
            {
                if (value.GetFrequency() <= UpdateRateUpperLimit) {
                    if (tempTime > value.getUpdateTime() && value.getSyncTime() < value.getUpdateTime()) {
                        tempTime = value.getUpdateTime();
                        keys.add(key);
                    }
                }
            }
        }


        return keys;
    }

    public List<String> GetOutOfSyncKeys()
    {
        List<String> keys = new ArrayList<>();

        for (Map.Entry<String,StateInfo> item : StateStore.entrySet())
        {
            StateInfo value = item.getValue();
            if (value.getUpdateTime() > value.getSyncTime())
            {
                keys.add(item.getKey());
            }
        }

        return keys;
    }

    public long GetLatestUpdateTime()
    {
        long time = 0;

        for (Map.Entry<String,StateInfo> item : StateStore.entrySet())
        {
            if (time < item.getValue().getUpdateTime())
            {
                time = item.getValue().getUpdateTime();
            }
        }

        return time;
    }

    public void PrintKeys()
    {
        for (Map.Entry<String,StateInfo> item : StateStore.entrySet())
        {
            StateInfo value = item.getValue();
            System.out.println("Key = " + item.getKey() + ", Size = " + value.getKeySize() + ", Update Counter = " + value.getUpdateCounter() + ", Update Frequency = " + value.getUpdateFrequency() + ", Sync Counter = " + value.getSynCounter());
        }
    }
}
