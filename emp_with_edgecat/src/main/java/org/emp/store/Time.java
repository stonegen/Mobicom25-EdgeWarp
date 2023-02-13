package org.emp.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Time {
    static long startTime;
    public static void Init()
    {
        startTime = System.currentTimeMillis();
    }

    public static long GetDurationMs(long startTime, long endTime)
    {
        return endTime - startTime;
    }
    public static long GetDurationSeconds(long startTime, long endTime)
    {
        return (long)((double)(endTime - startTime)/1000);
    }

    public static long GetCurrentTime()
    {
        long endTime = System.currentTimeMillis();
        return GetDurationMs(startTime, endTime);
    }
}