package com.bindiego.cdc;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.*;

public class CdcPipelineTest {

    @Test
    public void testFormatTimestamp() {
        Timestamp ts = Timestamp.valueOf("2026-08-26 11:05:30");
        String formatted = StreamingCdcPollerFn.formatTimestamp(ts);
        assertEquals("2026-08-26 11:05:30", formatted);

        assertNull(StreamingCdcPollerFn.formatTimestamp(null));
    }

    @Test
    public void testSequenceNumberCalculation() {
        TableRow row = new TableRow()
                .set("id", 1)
                .set("description", "Wireless Mouse")
                .set("price", 29.99)
                .set("created_at", "2026-08-26 10:00:00")
                .set("updated_at", "2026-08-26 11:05:30");

        Object updatedAtObj = row.get("updated_at");
        assertNotNull(updatedAtObj);
        assertTrue(updatedAtObj instanceof String);

        LocalDateTime ldt = LocalDateTime.parse((String) updatedAtObj, StreamingCdcPollerFn.DT_FORMATTER);
        long sequenceNumber = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();

        RowMutationInformation mutation = RowMutationInformation.of(
                RowMutationInformation.MutationType.UPSERT,
                sequenceNumber);

        assertEquals(RowMutationInformation.MutationType.UPSERT, mutation.getMutationType());
        assertNotNull(mutation.getChangeSequenceNumber());
        assertEquals(Long.toHexString(sequenceNumber), mutation.getChangeSequenceNumber());
        assertTrue(sequenceNumber > 0);
    }

    @Test
    public void testTableRowStructure() {
        TableRow row = new TableRow()
                .set("id", 42)
                .set("description", "Mechanical Keyboard")
                .set("price", 99.50)
                .set("created_at", "2026-08-26 10:00:00")
                .set("updated_at", "2026-08-26 11:05:30");

        assertEquals(Integer.valueOf(42), row.get("id"));
        assertEquals("Mechanical Keyboard", row.get("description"));
        assertEquals(Double.valueOf(99.50), row.get("price"));
        assertEquals("2026-08-26 10:00:00", row.get("created_at"));
        assertEquals("2026-08-26 11:05:30", row.get("updated_at"));
        assertNull("Should not contain dummy _sequence_number", row.get("_sequence_number"));
    }
}
