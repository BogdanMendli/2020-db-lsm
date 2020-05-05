package ru.mail.polis.bmendli;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Value implements Comparable<Value> {

    @Nullable
    private final ByteBuffer data;
    private final long timestamp;

    /**
     *  Creates new data wrapper, data may be null - tombstone.
     *
     * @param data - new data
     * @param timestamp - time when data was saved
     */
    public Value(@Nullable final ByteBuffer data, final long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
    }

    public Value(@Nullable final ByteBuffer data) {
        this(data, System.currentTimeMillis());
    }

    public Value(final long timestamp) {
        this(null, timestamp);
    }

    public Value() {
        this(null);
    }

    @Nullable
    public ByteBuffer getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isTombstone() {
        return data == null;
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return -Long.compare(timestamp, value.timestamp);
    }
}
