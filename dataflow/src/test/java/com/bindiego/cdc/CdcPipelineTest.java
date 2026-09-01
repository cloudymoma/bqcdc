package com.bindiego.cdc;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CdcPipelineTest {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    public void testParseJsonToTableRowUpsert() {
        String json = "{\"id\":1,\"description\":\"Mechanical Keyboard\",\"price\":129.99,"
                + "\"created_at\":\"2026-09-01 10:00:00\",\"updated_at\":\"2026-09-01 10:15:30\","
                + "\"_change_type\":\"UPSERT\",\"_sequence_number\":1788257730000}";

        PCollection<TableRow> output = p
                .apply(Create.of(json))
                .apply(ParDo.of(new CdcPipeline.ParseJsonToTableRowFn()));

        PAssert.that(output).satisfies(rows -> {
            TableRow row = rows.iterator().next();
            // TableRow values round-trip through the coder, so compare numerically
            assertEquals(1L, ((Number) row.get("id")).longValue());
            assertEquals("Mechanical Keyboard", row.get("description"));
            assertEquals(129.99, ((Number) row.get("price")).doubleValue(), 0.001);
            assertEquals("2026-09-01 10:00:00", row.get("created_at"));
            assertEquals("2026-09-01 10:15:30", row.get("updated_at"));
            assertEquals("UPSERT", row.get("_change_type"));
            assertEquals(1788257730000L, ((Number) row.get("_sequence_number")).longValue());
            return null;
        });

        p.run().waitUntilFinish();
    }

    @Test
    public void testParseJsonToTableRowDelete() {
        String json = "{\"id\":2,\"description\":\"USB-C Cable\",\"price\":19.99,"
                + "\"created_at\":\"2026-09-01 10:00:00\",\"updated_at\":\"2026-09-01 10:20:00\","
                + "\"_change_type\":\"DELETE\",\"_sequence_number\":1788258000000}";

        PCollection<TableRow> output = p
                .apply(Create.of(json))
                .apply(ParDo.of(new CdcPipeline.ParseJsonToTableRowFn()));

        PAssert.that(output).satisfies(rows -> {
            TableRow row = rows.iterator().next();
            assertEquals(2L, ((Number) row.get("id")).longValue());
            assertEquals("DELETE", row.get("_change_type"));
            return null;
        });

        p.run().waitUntilFinish();
    }

    @Test
    public void testExtractMutationInfoUpsert() {
        TableRow row = new TableRow()
                .set("id", 1L)
                .set("_change_type", "UPSERT")
                .set("_sequence_number", 123456789L);

        RowMutationInformation info = CdcPipeline.extractMutationInfo(row);
        assertNotNull(info);
        assertEquals(RowMutationInformation.MutationType.UPSERT, info.getMutationType());
        // of(type, long) encodes the sequence number as a hex change sequence number
        assertEquals(Long.toHexString(123456789L), info.getChangeSequenceNumber());
    }

    @Test
    public void testExtractMutationInfoDelete() {
        TableRow row = new TableRow()
                .set("id", 1L)
                .set("_change_type", "DELETE")
                .set("_sequence_number", 987654321L);

        RowMutationInformation info = CdcPipeline.extractMutationInfo(row);
        assertNotNull(info);
        assertEquals(RowMutationInformation.MutationType.DELETE, info.getMutationType());
        assertEquals(Long.toHexString(987654321L), info.getChangeSequenceNumber());
    }
}
