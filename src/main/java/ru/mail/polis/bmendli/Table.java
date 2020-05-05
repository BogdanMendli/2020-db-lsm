package ru.mail.polis.bmendli;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public interface Table {

    void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value);

    void remove(@NotNull final ByteBuffer key);

    @NotNull
    Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException;

    long size();

    void close() throws IOException;
}
