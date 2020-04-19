package ru.mail.polis.bogdanmendli;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return map
                .tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> Record.of(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        map.remove(key);
    }

    @Override
    public void close() {
        map.clear();
    }
}
