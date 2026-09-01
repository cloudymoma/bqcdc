package com.bindiego.cdc;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CdcPipelineOptionsTest {

    @Test
    public void testPipelineOptionsParsing() {
        String[] args = new String[] {
                "--pubsubSubscription=projects/test-proj/subscriptions/test-sub",
                "--bigQueryTable=test-proj:test_ds.test_tbl",
                "--gcsTempLocation=gs://test-bucket/temp"
        };

        CdcPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .as(CdcPipelineOptions.class);

        assertEquals("projects/test-proj/subscriptions/test-sub", options.getPubsubSubscription());
        assertEquals("test-proj:test_ds.test_tbl", options.getBigQueryTable());
        assertEquals("gs://test-bucket/temp", options.getGcsTempLocation());
    }
}
