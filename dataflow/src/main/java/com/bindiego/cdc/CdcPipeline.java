package com.bindiego.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * Streaming CDC Pipeline: Pub/Sub -> BigQuery Storage Write API with UPSERT & DELETE support.
 */
public class CdcPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(CdcPipeline.class);
    public static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * DoFn to parse JSON CDC message into a BigQuery TableRow.
     */
    public static class ParseJsonToTableRowFn extends DoFn<String, TableRow> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseJsonToTableRowFn.class);

        @ProcessElement
        public void processElement(@Element String jsonString, OutputReceiver<TableRow> out) {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(jsonString);

                long id = root.get("id").asLong();
                String description = root.hasNonNull("description") ? root.get("description").asText() : "";
                double price = root.hasNonNull("price") ? root.get("price").asDouble() : 0.0;
                String createdAt = root.hasNonNull("created_at") ? root.get("created_at").asText() : "";
                String updatedAt = root.hasNonNull("updated_at") ? root.get("updated_at").asText() : "";

                String changeType = root.hasNonNull("_change_type")
                        ? root.get("_change_type").asText().toUpperCase()
                        : "UPSERT";

                long seqNum;
                if (root.hasNonNull("_sequence_number")) {
                    seqNum = root.get("_sequence_number").asLong();
                } else if (!updatedAt.isEmpty()) {
                    try {
                        LocalDateTime ldt = LocalDateTime.parse(updatedAt, DT_FORMATTER);
                        seqNum = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    } catch (Exception e) {
                        seqNum = System.currentTimeMillis();
                    }
                } else {
                    seqNum = System.currentTimeMillis();
                }

                TableRow row = new TableRow()
                        .set("id", id)
                        .set("description", description)
                        .set("price", price)
                        .set("created_at", createdAt)
                        .set("updated_at", updatedAt)
                        .set("_change_type", changeType)
                        .set("_sequence_number", seqNum);

                LOG.info("Parsed CDC event: id={}, change_type={}, seq={}, desc='{}', price={}",
                        id, changeType, seqNum, description, price);

                out.output(row);
            } catch (Exception e) {
                LOG.error("Failed to parse JSON CDC message: '{}'. Error: {}", jsonString, e.getMessage(), e);
            }
        }
    }

    /**
     * Extracts mutation information for BigQuery Storage Write API CDC.
     */
    public static RowMutationInformation extractMutationInfo(TableRow row) {
        Object changeTypeObj = row.get("_change_type");
        String changeType = (changeTypeObj != null) ? changeTypeObj.toString().toUpperCase() : "UPSERT";

        RowMutationInformation.MutationType mutationType = "DELETE".equals(changeType)
                ? RowMutationInformation.MutationType.DELETE
                : RowMutationInformation.MutationType.UPSERT;

        long sequenceNumber;
        Object seqObj = row.get("_sequence_number");
        if (seqObj instanceof Number) {
            sequenceNumber = ((Number) seqObj).longValue();
        } else if (seqObj instanceof String) {
            try {
                sequenceNumber = Long.parseLong((String) seqObj);
            } catch (NumberFormatException e) {
                sequenceNumber = System.currentTimeMillis();
            }
        } else {
            sequenceNumber = System.currentTimeMillis();
        }

        return RowMutationInformation.of(mutationType, sequenceNumber);
    }

    public static void main(String[] args) {
        LOG.info("════════════════════════════════════════════════════════════════");
        LOG.info("  Starting Streaming CDC Pipeline: Pub/Sub → BigQuery");
        LOG.info("════════════════════════════════════════════════════════════════");

        CdcPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CdcPipelineOptions.class);

        LOG.info("Configuration:");
        LOG.info("  Pub/Sub Subscription: {}", options.getPubsubSubscription());
        LOG.info("  BigQuery Table:       {}", options.getBigQueryTable());
        LOG.info("  GCS Temp Location:    {}", options.getGcsTempLocation());
        LOG.info("════════════════════════════════════════════════════════════════");

        Pipeline pipeline = Pipeline.create(options);

        TableSchema bqSchema = new TableSchema()
                .setFields(Arrays.asList(
                        new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("description").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("price").setType("FLOAT").setMode("REQUIRED"),
                        new TableFieldSchema().setName("created_at").setType("DATETIME").setMode("REQUIRED"),
                        new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED")
                ));

        PCollection<String> jsonEvents = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription(options.getPubsubSubscription()));

        PCollection<TableRow> tableRows = jsonEvents.apply("ParseJsonToTableRow",
                ParDo.of(new ParseJsonToTableRowFn()));

        BigQueryIO.Write<TableRow> bqWriter = BigQueryIO.writeTableRows()
                .to(options.getBigQueryTable())
                .withSchema(bqSchema)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withPrimaryKey(ImmutableList.of("id"))
                .withRowMutationInformationFn(CdcPipeline::extractMutationInfo)
                // TableRow carries CDC metadata (_change_type, _sequence_number) that is
                // not part of the target schema - it must be ignored at write time
                .ignoreUnknownValues();

        if (options.getGcsTempLocation() != null && !options.getGcsTempLocation().isEmpty()) {
            bqWriter = bqWriter.withCustomGcsTempLocation(
                    StaticValueProvider.of(options.getGcsTempLocation()));
        }

        tableRows.apply("WriteToBigQueryCDC", bqWriter);

        LOG.info("Launching streaming pipeline...");
        pipeline.run();
    }
}
