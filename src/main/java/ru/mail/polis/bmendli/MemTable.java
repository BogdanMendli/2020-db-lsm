package ru.mail.polis.bmendli;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MemTable implements Table {

    @NotNull
    private final SortedMap<ByteBuffer, Value> map;
    private long size;

    /**
     * Memory storage for new data
     */
    public MemTable() {
        map = new TreeMap<>();
        size = 0;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        @Nullable final Value removedValue = map.put(key, new Value(value));
        size += value.remaining();
        if (removedValue == null) {
            size += key.remaining() + Long.BYTES;
        } else if (!removedValue.isTombstone()) {
            size -= removedValue.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        @Nullable final Value removedValue = map.put(key, new Value());
        if (removedValue == null) {
            size += key.remaining() + Long.BYTES;
        } else if (!removedValue.isTombstone()) {
            size -= removedValue.getData().remaining();
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map
                .tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue().getData()))
                .iterator();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void close() {
        map.clear();
        size = 0;
    }
}
