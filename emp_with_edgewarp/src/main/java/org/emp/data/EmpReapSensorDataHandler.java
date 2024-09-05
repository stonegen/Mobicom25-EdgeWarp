package org.emp.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.emp.edge.REAP;
import org.emp.edge.UploadingScheduler;
import org.emp.network.BandwidthEstimator;
import org.emp.network.NonBlockingNetworkServer;
import org.emp.store.StateMigration;
import org.emp.utils.DataUtils;
import org.emp.utils.VoronoiAdapt;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A sensor data handler that uses EMP REAP point cloud partitioning algorithm
 */
public class EmpReapSensorDataHandler implements SensorDataHandler {
  private static final Logger LOGGER = LogManager.getLogger(EmpReapSensorDataHandler.class);


  // EMP statistics handler to store system statistics during running
  StatHandler statHandler = new StatHandler();
  // Network server instance to send control messages to vehicles
  NonBlockingNetworkServer networkServer;
  // Bandwidth estimator instance
  private final BandwidthEstimator bandwidthEstimator;
  // ID of the partitioning algorithm to use (1: baseline; 2: naive; 3: bw-only; 4: bw+adapt)
  private final int algorithmId;
  // Path to save the merged point cloud for replay
  private final String savePath;
  // Mapping of vehicle ID to {@code VehicleState} instance
  private final Map<Integer, VehicleState> vehicleStateMap = new HashMap<>();
  // frameID of the frame of which the primary vehicle (v1) delivered its oxts of not
  private final Set<Integer> isOxtsReceivedSet = new HashSet<>();
  // frameID -> < vehicleID -> Set<chunkID> >
  private final Map<Integer, Map<Integer, Set<Integer>>> unmergedData = new HashMap<>();
  // frameID -> Oxts of the primary vehicle
  private final Map<Integer, VehicleLocation> primaryOxtsMap = new HashMap<>();
  // frameID -> merged point cloud
  private final Map<Integer, FloatBuffer> mergedPointCloudMap = new HashMap<>();
  // frameID -> merged point cloud buffer offset
  private final Map<Integer, Integer> mergedPointCloudOffsetMap = new HashMap<>();
  // ID of the frame that the system is working on
  private int currentFrame = 0;
  // Max number of clients
  private int maxNumClients = 1;

  private StateMigration stateMigration;



  public EmpReapSensorDataHandler(BandwidthEstimator bandwidthEstimator, int algorithmId, String savePath, StateMigration _stateMigration) {
    this.bandwidthEstimator = bandwidthEstimator;
    this.algorithmId = algorithmId;
    this.savePath = savePath;
    this.stateMigration = _stateMigration;
  }

  @Override
  public void pushUnmergedData(int vehicleId, int frameId, int chunkId) {
    Map<Integer, Set<Integer>> vehicleIdMap = unmergedData.get(frameId);
    if (vehicleIdMap == null) {
      vehicleIdMap = new HashMap<>();
      Set<Integer> chunkIdSet = new HashSet<>();
      chunkIdSet.add(chunkId);
      vehicleIdMap.put(vehicleId, chunkIdSet);
    }
    else {
      Set<Integer> chunkIdSet = vehicleIdMap.get(vehicleId);
      if (chunkIdSet == null) { chunkIdSet = new HashSet<>();}
      chunkIdSet.add(chunkId);
      vehicleIdMap.put(vehicleId, chunkIdSet);
    }
    unmergedData.put(frameId, vehicleIdMap);
  }

  @Override
  public Map<Integer, Set<Integer>> popUnmergedData(int frameId) { return unmergedData.remove(frameId); }

  @Override
  public boolean shouldRunMerging(int frameId) { return isOxtsReceivedSet.contains(frameId); }

  @Override
  public float[] getDataChunk(int vehicleId, int frameId, int chunkId) {
    return vehicleStateMap.get(vehicleId).getFrames().get(frameId).getChunks().get(chunkId).getDecodedPointCloud();
  }

  @Override
  public VehicleLocation getPrimaryLocation (int frameId) { return primaryOxtsMap.get(frameId); }

  @Override
  public VehicleLocation getVehicleLocation(int vehicleId, int frameId) {
    return vehicleStateMap.get(vehicleId).getFrames().get(frameId).getVehicleLocation();
  }

  @Override
  public FloatBuffer getMergedPointCloud (int frameId) {
    synchronized (mergedPointCloudMap) {
      return mergedPointCloudMap.computeIfAbsent(frameId, k -> FloatBuffer.allocate(533248 * maxNumClients));
    }
  }

  @Override
  public int updateMergedPointCloudOffset(int frameId, int dataSize) {
    synchronized (mergedPointCloudOffsetMap) {
      int offset = mergedPointCloudOffsetMap.computeIfAbsent(frameId, k -> 0);
      mergedPointCloudOffsetMap.put(frameId, offset + dataSize);
      return offset;
    }
  }

  @Override
  // Amir: New received chunk is being added to the frame of the vehicle.
  public void saveDataChunk(SensorDataChunk dataChunk) {
    int vehicleId = dataChunk.getVehicleId();
    int frameId = dataChunk.getFrameId();
    int chunkId = dataChunk.getChunkId();
    Map<Integer, SensorDataChunk> chunks = getFrame(vehicleId, frameId).getChunks();
    if (!chunks.containsKey(chunkId)) {
      // Amir: Print the size of the received chunk
      LOGGER.info("Size: " + dataChunk.getCompressedPointCloud().length);
      // todo: STORE
      chunks.put(chunkId, dataChunk);
      LOGGER.info("---> No of chunks: " + chunks.size());
      LOGGER.info("---> No of frames: " + vehicleStateMap.get(vehicleId).getFrames().size());

      try {
        System.out.println("Saving " + frameId + " chunk to local datastore");
        this.stateMigration.saveNewFrameChunk("frame" + String.valueOf(frameId), dataChunk.getCompressedPointCloudInString());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      this.stateMigration.Process(frameId);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void saveMergedPointCloud(int frameId, Set<Integer> vehicleIds) {
    for (Integer vehicleId : vehicleIds) {
      vehicleStateMap.get(vehicleId).getFrames().get(frameId).setMerged(true);
    }
  }

  @Override
  public void updateVehicleLocation(int vehicleId, int frameId, VehicleLocation location){
    // Update the location data for the corresponding vehicle, frame
    getFrame(vehicleId, frameId).setVehicleLocation(location);

    // When the location data of the primary vehicle (v1) is received
    if (vehicleId == 1) {
      primaryOxtsMap.put(frameId, location);
      isOxtsReceivedSet.add(frameId);
    }
  }

  @Override
  public void updatePartitioningDecisions(int frameId) throws IOException {
    // Map vehicle ID (element) to ordered index (index) for REAP algorithm
    List<Integer> orderedVehicleIds = new ArrayList<>(vehicleStateMap.keySet());

    // Get latest vehicle locations and estimated bandwidths
    List<float[]> oxtsList = new ArrayList<>();
    List<Float> bwList = new ArrayList<>();
    for (Integer vehicleId : orderedVehicleIds) {
      oxtsList.add(getVehicleLocation(vehicleId, frameId).getOxtsData());
      float bw = (float)bandwidthEstimator.getEstimatedBW(vehicleId, "naive") / 1000;
      if (bw < 0) {
        bw = 10f;
      }
      LOGGER.info("[BW] vehicle: " + vehicleId + ", frame: " + frameId + ", bandwidth: " + bw);
      bwList.add(bw);
    }

    // Run REAP to get the partitioning decisions
    VoronoiAdapt.partitionDecision decisions = REAP.reap(algorithmId, oxtsList, bwList);

    // Save the latest neighboring relationship and send the partitioning decisions to vehicles
    List<ArrayList<Integer>> neighborList = decisions.neighbors;
    List<ArrayList<float[]>> decisionList = decisions.pbSet;
    for (int i = 0; i < orderedVehicleIds.size(); i++) {
      int vehicleId = orderedVehicleIds.get(i);
      Set<Integer> neighborIds = new HashSet<>();
      for (Integer j : neighborList.get(i)) {
        neighborIds.add(orderedVehicleIds.get(j));
      }
      vehicleStateMap.get(vehicleId).setNeighborIds(neighborIds);
      LOGGER.debug("[REAP] vehicle: " + vehicleId + " - neighbors: " + vehicleStateMap.get(vehicleId).getNeighborIds());

      List<float[]> decision = decisionList.get(i);
      byte[] decisionBytes = VehicleMessageHandlerImpl.floatArrayListToByteArray(decision);
      byte[] decisionMsg = VehicleMessageHandlerImpl.encodeMessage(decisionBytes, currentFrame, 'M');
      networkServer.putDataToSendByteBufferQueue(vehicleId, decisionMsg);
    }
  }

  @Override
  public boolean shouldRunObjectDetection() throws IOException {
    if (UploadingScheduler.check(vehicleStateMap, currentFrame, statHandler)) {
      statHandler.logFrameEndTime(currentFrame, System.currentTimeMillis());

      // Send "finish" signal to vehicles
      byte[] finishMsg = VehicleMessageHandlerImpl.encodeMessage(new byte[]{}, currentFrame, 'D');
      for (Integer vehicleId : vehicleStateMap.keySet()) {
        LOGGER.debug("frame: " + currentFrame + " finish signal sent to vehicle: " + vehicleId);
        networkServer.putDataToSendByteBufferQueue(vehicleId, finishMsg);
        // Amir: Log size of the vehicle states
//        vehicleStateMap.get().
      }

      // Update partitioning decisions and send to vehicles
      updatePartitioningDecisions(currentFrame);
      LOGGER.info("frame: " + currentFrame + " uploading finished");

      // Save merged point cloud to file
      if (savePath != null) {
        long tSave1 = System.currentTimeMillis();
        float[] mergedPointCloud = new float[mergedPointCloudOffsetMap.get(currentFrame)];
        mergedPointCloudMap.get(currentFrame).get(mergedPointCloud, 0, mergedPointCloudOffsetMap.get(currentFrame));
        DataUtils.writePointCloudToFile(mergedPointCloud, savePath + String.format("%06d", currentFrame) + ".bin");
        long tSave2 = System.currentTimeMillis();
        statHandler.logDataSavingTime(currentFrame, tSave2 - tSave1);
      }

      currentFrame ++;
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public void setNetworkServer(NonBlockingNetworkServer networkServer) {
    if (networkServer == null) {
      LOGGER.error("Network server not built");
    }
    else {
      this.networkServer = networkServer;
      this.maxNumClients = networkServer.maxNumClients;
    }
  }

  private VehicleState getVehicle (int vehicleId) {
    VehicleState state = vehicleStateMap.get(vehicleId);
    if (state == null) {
      state = VehicleState.builder().vehicleId(vehicleId).frames(new HashMap<>()).build();
      vehicleStateMap.put(vehicleId, state);
    }
    return state;
  }

  private SensorDataFrame getFrame (int vehicleId, int frameId) {
    VehicleState state = vehicleStateMap.get(vehicleId);
    if (state == null) {
      state = VehicleState.builder().vehicleId(vehicleId).frames(new HashMap<>()).build();
      vehicleStateMap.put(vehicleId, state);
    }
    SensorDataFrame frame = state.getFrames().get(frameId);
    if (frame == null) {
      frame = SensorDataFrame.builder().chunks(new HashMap<>()).build();
      state.getFrames().put(frameId, frame);
    }
    return frame;
  }
}