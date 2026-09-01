package com.bindiego.cdc;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Pipeline options for Pub/Sub to BigQuery CDC Pipeline.
 */
public interface CdcPipelineOptions extends DataflowPipelineOptions {

    @Description("Pub/Sub subscription to read from (format: projects/<project>/subscriptions/<subscription>)")
    @Validation.Required
    String getPubsubSubscription();
    void setPubsubSubscription(String value);

    @Description("BigQuery table spec (format: project:dataset.table)")
    @Validation.Required
    String getBigQueryTable();
    void setBigQueryTable(String value);

    @Description("GCS temp location for BigQuery Storage Write API staging")
    String getGcsTempLocation();
    void setGcsTempLocation(String value);
}
