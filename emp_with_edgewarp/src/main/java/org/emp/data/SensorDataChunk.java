package org.emp.data;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.emp.task.Task.TaskType;

/**
 * Stores information of a sensor data chunk from a vehicle
 */
// Amir: SensorDataChunk
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder
public class SensorDataChunk {
  TaskType taskFinished;

  int vehicleId;
  int frameId;
  int chunkId;
  byte[] compressedPointCloud;
  float[] decodedPointCloud;
  boolean isUsed;

  public int logChunkSize() {
    return chunkId;
  }

  public String getCompressedPointCloudInString() {
    return new String(compressedPointCloud);
  }
}
