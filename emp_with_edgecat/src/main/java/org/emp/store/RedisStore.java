package org.emp.store;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import sun.awt.Mutex;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class RedisStore implements AutoCloseable {
    private String ip;
    private int port;
    private boolean isSourceStore;

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private AtomicReference<StateTracker> stateTracker;
    private boolean connected;
    private final String password = "testpass";

    RedisStore(String ipaddress, int portNumber, AtomicReference<StateTracker> tracker, boolean sourceStore) {
        ip = ipaddress;
        port = portNumber;
        stateTracker = tracker;
        isSourceStore = sourceStore;
        connected = false;
        redisClient = RedisClient.create("redis://" + password + "@" + ip + ":" + port);
    }

    @Override
    public void close() throws Exception {
        redisClient.close();
        System.out.println("Client disconnected");
    }

    public void Connect() {
        System.out.println("Connecting to Redis store on IP: " + ip + " Port:" + port);
        connection = redisClient.connect();
        System.out.println("Connected to Redis store.");
    }

    public void Disconnect() {
        redisClient.close();
    }

    public boolean Set(String key, String data) throws InterruptedException {
        RedisCommands<String, String> syncCommands = connection.sync();
        syncCommands.set(key, data);

        if (isSourceStore) {
            StateTracker updatedTracker = stateTracker.get();
            updatedTracker.UpdateKey(key);
            stateTracker.set(updatedTracker);
        }

        return true;
    }

    public boolean HasKey(String key) {
        RedisCommands<String, String> syncCommands = connection.sync();
        String val = syncCommands.get(key);
        return val == null ? false : true;
    }

    public String Get(String key) throws InterruptedException, ExecutionException {
        RedisAsyncCommands<String, String> commands = connection.async();
        RedisFuture<String> future = commands.get(key);
        String value = future.get();

        return value;
    }
}
