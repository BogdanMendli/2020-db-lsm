package ru.mail.polis.bmendli;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    public static final int NO_EXPIRATION = 0;
    public static final int TOMBSTONE_EXPIRE_TIME_MS = 10_000;

    @Nullable
    private final ByteBuffer data;
    private final long timestamp;
    private final long expireTime;

    /**
     *  Creates new data wrapper, data may be null - tombstone.
     *
     * @param data - new data
     * @param timestamp - time when data was saved
     */
    public Value(@Nullable final ByteBuffer data, final long timestamp, final long expireTime) {
        this.data = data;
        this.timestamp = timestamp;
        this.expireTime = expireTime;
    }

    public Value(@Nullable final ByteBuffer data, final long expireTime) {
        this(data, System.currentTimeMillis(), expireTime);
    }

    public Value(final long timestamp, final long expireTime) {
        this(null, timestamp, expireTime);
    }

    public Value(final long expireTime) {
        this(null, expireTime);
    }

    public Value(@Nullable final ByteBuffer data) {
        this(data, NO_EXPIRATION);
    }

    public Value() {
        this(NO_EXPIRATION);
    }

    @Nullable
    public ByteBuffer getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public boolean isTombstone() {
        return data == null;
    }

    public boolean isExpire() {
        return expireTime > NO_EXPIRATION && timestamp + expireTime < System.currentTimeMillis();
    }

    public boolean isExpiredTombstone() {
        return isTombstone() && timestamp + TOMBSTONE_EXPIRE_TIME_MS < System.currentTimeMillis();
    }
    
    @Override
    public int compareTo(@NotNull final Value value) {
        return -Long.compare(timestamp, value.timestamp);
    }
}
