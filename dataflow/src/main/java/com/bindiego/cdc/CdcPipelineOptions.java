/*
 * CDC Pipeline Options for BigQuery CDC Dataflow Pipeline
 */
package com.bindiego.cdc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * CDC Pipeline Options
 */
public interface CdcPipelineOptions extends PipelineOptions {
    @Description("MySQL JDBC URL")
    @Required
    String getMysqlJdbcUrl();
    void setMysqlJdbcUrl(String value);

    @Description("MySQL username")
    @Default.String("root")
    String getMysqlUsername();
    void setMysqlUsername(String value);

    @Description("MySQL password")
    @Required
    String getMysqlPassword();
    void setMysqlPassword(String value);

    @Description("MySQL database name")
    @Required
    String getMysqlDatabase();
    void setMysqlDatabase(String value);

    @Description("MySQL table name")
    @Required
    String getMysqlTable();
    void setMysqlTable(String value);

    @Description("BigQuery output table (format: project:dataset.table)")
    @Required
    String getBigQueryTable();
    void setBigQueryTable(String value);

    @Description("Polling interval in seconds - how often to check for changes")
    @Default.Integer(10)
    Integer getPollingIntervalSeconds();
    void setPollingIntervalSeconds(Integer value);

    @Description("GCS temp location for BigQuery")
    @Required
    String getGcsTempLocation();
    void setGcsTempLocation(String value);

    @Description("Behavior when in-memory timestamp is null at startup: " +
                 "true = full table sync then record timestamp, " +
                 "false = only record max timestamp and wait for next poll")
    @Default.Boolean(false)
    Boolean getUpdateAllIfTsNull();
    void setUpdateAllIfTsNull(Boolean value);
}
