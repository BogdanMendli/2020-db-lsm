package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExpirationTest extends TestBase {

    @Test
    void allExpire(@TempDir File data) throws IOException, InterruptedException {
        final ByteBuffer firstKey = randomKey();
        final ByteBuffer secondKey = randomKey();
        final ByteBuffer thirdKey = randomKey();
        final ByteBuffer fourthKey = randomKey();
        final ByteBuffer fifthKey = randomKey();

        try (DAO dao = DAOFactory.create(data)) {

            final int expireTime = randomExpireTime(1000);

            dao.upsert(firstKey, randomValue(), expireTime);
            dao.upsert(secondKey, randomValue(), expireTime);
            dao.upsert(thirdKey, randomValue(), expireTime);
            dao.upsert(fourthKey, randomValue(), expireTime);
            dao.upsert(fifthKey, randomValue(), expireTime);

            Thread.sleep(expireTime);

            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(thirdKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(fourthKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(fifthKey));
        }

        final int expireTime = randomExpireTime(2000);

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(firstKey, randomValue(), expireTime);
            dao.upsert(secondKey, randomValue(), expireTime);
            dao.upsert(thirdKey, randomValue(), expireTime);
            dao.upsert(fourthKey, randomValue(), expireTime);
            dao.upsert(fifthKey, randomValue(), expireTime);
        }

        Thread.sleep(expireTime);

        try (DAO dao = DAOFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(thirdKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(fourthKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(fifthKey));
        }
    }

    @Test
    void someDataIsExpire(@TempDir File data) throws IOException, InterruptedException {
        final ByteBuffer firstKey = randomKey();
        final ByteBuffer secondKey = randomKey();
        final ByteBuffer thirdKey = randomKey();
        final ByteBuffer fourthKey = randomKey();
        final ByteBuffer fifthKey = randomKey();
        final ByteBuffer sixthKey = randomKey();

        final ByteBuffer firstValue = randomValue();
        final ByteBuffer secondValue = randomValue();
        final ByteBuffer thirdValue = randomValue();
        final ByteBuffer fourthValue = randomValue();
        final ByteBuffer fifthValue = randomValue();
        final ByteBuffer sixthValue = randomValue();

        final int firstExpireTime = randomExpireTime(1000);
        final int secondExpireTime = 5_000;

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(firstKey, firstValue, firstExpireTime);
            dao.upsert(secondKey, secondValue, firstExpireTime);
            dao.upsert(thirdKey, thirdValue, secondExpireTime);
            dao.upsert(fourthKey, fourthValue, secondExpireTime);
            dao.upsert(fifthKey, fifthValue);
            dao.upsert(sixthKey, sixthValue);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));
            assertEquals(thirdValue, dao.get(thirdKey));
            assertEquals(fourthValue, dao.get(fourthKey));
            assertEquals(fifthValue, dao.get(fifthKey));
            assertEquals(sixthValue, dao.get(sixthKey));

            Thread.sleep(firstExpireTime);

            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
            assertEquals(thirdValue, dao.get(thirdKey));
            assertEquals(fourthValue, dao.get(fourthKey));
            assertEquals(fifthValue, dao.get(fifthKey));
            assertEquals(sixthValue, dao.get(sixthKey));
        }

        Thread.sleep(secondExpireTime - firstExpireTime);

        try (DAO dao = DAOFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(thirdKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(fourthKey));
            assertEquals(fifthValue, dao.get(fifthKey));
            assertEquals(sixthValue, dao.get(sixthKey));
        }
    }

    @Test
    void overwrite(@TempDir File data) throws IOException, InterruptedException {
        final ByteBuffer firstKey = randomKey();
        final ByteBuffer secondKey = randomKey();

        ByteBuffer firstValue = randomValue();
        ByteBuffer secondValue = randomValue();

        final int firstExpireTime = randomExpireTime(3000);

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(firstKey, firstValue, firstExpireTime);
            dao.upsert(secondKey, secondValue, firstExpireTime);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            Thread.sleep(firstExpireTime);

            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
        }

        firstValue = randomValue();
        secondValue = randomValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(firstKey, firstValue);
            dao.upsert(secondKey, secondValue);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            Thread.sleep(firstExpireTime);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));
        }

        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            firstValue = randomValue();
            secondValue = randomValue();

            dao.upsert(firstKey, firstValue, firstExpireTime);
            dao.upsert(secondKey, secondValue);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            Thread.sleep(firstExpireTime);

            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));
        }

        try (DAO dao = DAOFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            firstValue = randomValue();
            secondValue = randomValue();

            dao.upsert(firstKey, firstValue);
            dao.upsert(secondKey, secondValue, firstExpireTime);

            assertEquals(firstValue, dao.get(firstKey));
            assertEquals(secondValue, dao.get(secondKey));

            Thread.sleep(firstExpireTime);

            assertEquals(firstValue, dao.get(firstKey));
            assertThrows(NoSuchElementException.class, () -> dao.get(secondKey));
        }
    }
}
