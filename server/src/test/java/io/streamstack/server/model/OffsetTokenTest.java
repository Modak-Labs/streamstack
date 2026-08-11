package io.streamstack.server.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetTokenTest {
    @Test
    void zeroPaddedAndMonotonic() {
        OffsetToken a = OffsetToken.ofRecordOffset(1);
        OffsetToken b = OffsetToken.ofRecordOffset(2);
        assertTrue(a.value().compareTo(b.value()) < 0);
        assertEquals(20, a.value().length());
        assertEquals(1, OffsetToken.parse(a.value()).recordOffset());
        assertEquals(0, OffsetToken.parse("-1").recordOffset());
    }

    @Test
    void rejectsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> OffsetToken.parse("abc"));
    }
}
