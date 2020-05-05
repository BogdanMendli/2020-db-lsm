package ru.mail.polis.bmendli;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Cell implements Comparable<Cell> {

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    public Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public Cell(@NotNull final ByteBuffer key, @Nullable final ByteBuffer value) {
        this(key, new Value(value));
    }

    public Cell(@NotNull final ByteBuffer key) {
        this(key, new Value(null));
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull final Cell cell) {
        final int resultCmp = key.compareTo(cell.key);
        return resultCmp == 0 ? value.compareTo(cell.value) : resultCmp;
    }
}
