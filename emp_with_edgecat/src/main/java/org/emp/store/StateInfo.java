package org.emp.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StateInfo {
    long updateTime;
    long syncTime;
    long creationTime;
    String key;
    long updateCounter;
    long synCounter;
    long keySize;
    float updateFrequency;

    StateInfo(String newKey)
    {
        updateTime = System.currentTimeMillis();
        syncTime = System.currentTimeMillis();
        creationTime = System.currentTimeMillis();
        key = newKey;
        updateCounter = 0;
        synCounter = 0;
        keySize = 0;
        updateFrequency = 0;
    }

    public float GetFrequency()
    {
        float frequency = 0;

        // Wait for a few updates before getting frequency
        if (updateCounter >= 3)
        {
            long timeNow = System.currentTimeMillis();
            frequency = (float)(timeNow - creationTime)/(float)updateCounter;
        }

        return frequency;
    }
}
