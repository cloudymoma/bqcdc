package com.bindiego.cdc;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class CdcPipelineOptionsTest {

    @Test
    public void testDefaultValues() {
        CdcPipelineOptions options = PipelineOptionsFactory.as(CdcPipelineOptions.class);

        assertEquals("root", options.getMysqlUsername());
        assertEquals(Integer.valueOf(10), options.getPollingIntervalSeconds());
        assertEquals(Boolean.FALSE, options.getUpdateAllIfTsNull());
    }

    @Test
    public void testCustomValuesFromArgs() {
        String[] args = new String[]{
                "--mysqlJdbcUrl=jdbc:mysql://127.0.0.1:3306",
                "--mysqlUsername=custom_user",
                "--mysqlPassword=secret",
                "--mysqlDatabase=test_db",
                "--mysqlTable=test_table",
                "--bigQueryTable=myproject:mydataset.mytable",
                "--gcsTempLocation=gs://mybucket/temp",
                "--pollingIntervalSeconds=30",
                "--updateAllIfTsNull=true"
        };

        CdcPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(CdcPipelineOptions.class);

        assertEquals("jdbc:mysql://127.0.0.1:3306", options.getMysqlJdbcUrl());
        assertEquals("custom_user", options.getMysqlUsername());
        assertEquals("secret", options.getMysqlPassword());
        assertEquals("test_db", options.getMysqlDatabase());
        assertEquals("test_table", options.getMysqlTable());
        assertEquals("myproject:mydataset.mytable", options.getBigQueryTable());
        assertEquals("gs://mybucket/temp", options.getGcsTempLocation());
        assertEquals(Integer.valueOf(30), options.getPollingIntervalSeconds());
        assertEquals(Boolean.TRUE, options.getUpdateAllIfTsNull());
    }
}
