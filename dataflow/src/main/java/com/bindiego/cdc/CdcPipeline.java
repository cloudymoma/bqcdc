/*
 * Streaming CDC Pipeline for MySQL to BigQuery
 *
 * This pipeline continuously polls MySQL for changes based on the updated_at column
 * and syncs only the changed records to BigQuery.
 *
 * Key Features:
 * - Configurable polling interval (in seconds)
 * - Tracks last processed timestamp in memory
 * - Supports two startup modes via updateAllIfTsNull flag:
 *   - true: Full table sync on first run, then incremental
 *   - false: Skip existing data, only capture new changes
 */
package com.bindiego.cdc;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * Streaming CDC Poller DoFn
 *
 * This DoFn maintains state for the last processed timestamp and implements
 * the core CDC logic:
 *
 * 1. If lastTimestamp is NULL (first run):
 *    - If updateAllIfTsNull=true: Fetch ALL records, sync to BQ, record max(updated_at)
 *    - If updateAllIfTsNull=false: Query max(updated_at), record it, emit nothing (wait for next poll)
 *
 * 2. If lastTimestamp is NOT NULL (subsequent runs):
 *    - Fetch records WHERE updated_at > lastTimestamp
 *    - Sync to BQ
 *    - Update lastTimestamp to max(updated_at) from fetched records
 */
class StreamingCdcPollerFn extends DoFn<KV<String, Long>, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingCdcPollerFn.class);
    private static final DateTimeFormatter DT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final boolean updateAllIfTsNull;

    // State to track the last processed updated_at timestamp (stored as epoch millis)
    @StateId("lastUpdatedAt")
    private final StateSpec<ValueState<Long>> lastUpdatedAtSpec = StateSpecs.value();

    // State to track if this is the first poll (to handle updateAllIfTsNull logic)
    @StateId("isFirstPoll")
    private final StateSpec<ValueState<Boolean>> isFirstPollSpec = StateSpecs.value();

    public StreamingCdcPollerFn(String jdbcUrl, String username, String password,
                                 String database, String table, boolean updateAllIfTsNull) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.updateAllIfTsNull = updateAllIfTsNull;
    }

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("lastUpdatedAt") ValueState<Long> lastUpdatedAtState,
                               @StateId("isFirstPoll") ValueState<Boolean> isFirstPollState) {

        Long pollSequence = c.element().getValue();
        Long lastUpdatedAt = lastUpdatedAtState.read();
        Boolean isFirstPoll = isFirstPollState.read();

        // Initialize first poll flag
        if (isFirstPoll == null) {
            isFirstPoll = true;
        }

        String fullJdbcUrl = String.format("%s/%s?useSSL=false&allowPublicKeyRetrieval=true",
                jdbcUrl, database);

        LOG.info("════════════════════════════════════════════════════════════");
        LOG.info("Poll #{}: Starting CDC check", pollSequence);
        LOG.info("  Last updated_at: {}", lastUpdatedAt == null ? "NULL (first run)" :
                new java.sql.Timestamp(lastUpdatedAt).toString());
        LOG.info("  Is first poll: {}", isFirstPoll);
        LOG.info("  updateAllIfTsNull: {}", updateAllIfTsNull);
        LOG.info("════════════════════════════════════════════════════════════");

        LOG.info("Connecting to MySQL: {}/{}", jdbcUrl, database);
        try (Connection conn = DriverManager.getConnection(fullJdbcUrl, username, password)) {
            LOG.info("MySQL connection established successfully");

            // CASE 1: First poll with null timestamp
            if (isFirstPoll && lastUpdatedAt == null) {
                handleFirstPoll(conn, c, lastUpdatedAtState, isFirstPollState);
                return;
            }

            // CASE 2: Subsequent polls - fetch only records newer than lastUpdatedAt
            handleIncrementalPoll(conn, c, lastUpdatedAt, lastUpdatedAtState, pollSequence);

        } catch (Exception e) {
            LOG.error("Error during CDC poll: {}", e.getMessage(), e);
            throw new RuntimeException("CDC poll failed", e);
        }
    }

    /**
     * Handle the first poll when lastUpdatedAt is null.
     * Behavior depends on updateAllIfTsNull flag.
     */
    private void handleFirstPoll(Connection conn, ProcessContext c,
                                  ValueState<Long> lastUpdatedAtState,
                                  ValueState<Boolean> isFirstPollState) throws Exception {

        if (updateAllIfTsNull) {
            // updateAllIfTsNull=true: Fetch ALL records and sync to BigQuery
            LOG.info("[FIRST POLL] updateAllIfTsNull=true - Performing FULL TABLE SYNC");

            String query = String.format(
                    "SELECT id, description, price, created_at, updated_at FROM %s ORDER BY updated_at",
                    table);

            long maxUpdatedAt = 0;
            int count = 0;

            try (PreparedStatement stmt = conn.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    TableRow row = resultSetToTableRow(rs);
                    c.output(row);
                    count++;

                    java.sql.Timestamp updatedAt = rs.getTimestamp("updated_at");
                    if (updatedAt != null && updatedAt.getTime() > maxUpdatedAt) {
                        maxUpdatedAt = updatedAt.getTime();
                    }
                }
            }

            LOG.info("[FIRST POLL] Emitting {} records to BigQuery (CDC UPSERT)", count);

            // Record the max timestamp (or current time if no records)
            if (maxUpdatedAt > 0) {
                lastUpdatedAtState.write(maxUpdatedAt);
                LOG.info("[FIRST POLL] Recorded max updated_at: {}", new java.sql.Timestamp(maxUpdatedAt));
            } else {
                // No records found, record current time
                long now = System.currentTimeMillis();
                lastUpdatedAtState.write(now);
                LOG.info("[FIRST POLL] No records found, using current time: {}", new java.sql.Timestamp(now));
            }

        } else {
            // updateAllIfTsNull=false: Only record max(updated_at), don't sync anything
            LOG.info("[FIRST POLL] updateAllIfTsNull=false - Recording timestamp only, NO SYNC");

            String query = String.format("SELECT MAX(updated_at) as max_ts FROM %s", table);

            try (PreparedStatement stmt = conn.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {

                if (rs.next()) {
                    java.sql.Timestamp maxTs = rs.getTimestamp("max_ts");
                    if (maxTs != null) {
                        lastUpdatedAtState.write(maxTs.getTime());
                        LOG.info("[FIRST POLL] Recorded max updated_at: {} - Will capture changes AFTER this",
                                maxTs);
                    } else {
                        // Empty table, record current time
                        long now = System.currentTimeMillis();
                        lastUpdatedAtState.write(now);
                        LOG.info("[FIRST POLL] Empty table, using current time: {}", new java.sql.Timestamp(now));
                    }
                }
            }

            LOG.info("[FIRST POLL] No data synced. Waiting for next poll to capture new changes.");
        }

        // Mark first poll as complete
        isFirstPollState.write(false);
    }

    /**
     * Handle incremental polls - fetch only records where updated_at > lastUpdatedAt
     */
    private void handleIncrementalPoll(Connection conn, ProcessContext c,
                                        Long lastUpdatedAt, ValueState<Long> lastUpdatedAtState,
                                        Long pollSequence) throws Exception {

        java.sql.Timestamp lastTs = new java.sql.Timestamp(lastUpdatedAt);

        // Query for records updated after the last timestamp
        // Using > (greater than) to avoid re-processing the same record
        String query = String.format(
                "SELECT id, description, price, created_at, updated_at FROM %s " +
                        "WHERE updated_at > ? ORDER BY updated_at",
                table);

        long maxUpdatedAt = lastUpdatedAt;
        int count = 0;

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setTimestamp(1, lastTs);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TableRow row = resultSetToTableRow(rs);
                    c.output(row);
                    count++;

                    java.sql.Timestamp updatedAt = rs.getTimestamp("updated_at");
                    if (updatedAt != null && updatedAt.getTime() > maxUpdatedAt) {
                        maxUpdatedAt = updatedAt.getTime();
                    }

                    LOG.info("  [CHANGE] id={}, price={}, updated_at={}",
                            rs.getInt("id"), rs.getDouble("price"), updatedAt);
                }
            }
        }

        if (count > 0) {
            // Update the last timestamp
            lastUpdatedAtState.write(maxUpdatedAt);
            LOG.info("Poll #{}: Emitting {} records to BigQuery (CDC UPSERT)", pollSequence, count);
            LOG.info("Poll #{}: Updated watermark to {}", pollSequence, new java.sql.Timestamp(maxUpdatedAt));
        } else {
            LOG.info("Poll #{}: No changes detected since {} - skipping BigQuery write", pollSequence, lastTs);
        }
    }

    /**
     * Convert a ResultSet row to a BigQuery TableRow.
     * CDC UPSERT is handled via RowMutationInformation in the BigQueryIO write.
     * We store the updated_at timestamp as _sequence_number for CDC ordering.
     */
    private TableRow resultSetToTableRow(ResultSet rs) throws Exception {
        java.sql.Timestamp updatedAt = rs.getTimestamp("updated_at");
        long sequenceNumber = updatedAt != null ? updatedAt.getTime() : System.currentTimeMillis();

        return new TableRow()
                .set("id", rs.getInt("id"))
                .set("description", rs.getString("description"))
                .set("price", rs.getDouble("price"))
                .set("created_at", formatTimestamp(rs.getTimestamp("created_at")))
                .set("updated_at", formatTimestamp(updatedAt))
                .set("_sequence_number", sequenceNumber);  // Used for CDC ordering
    }

    private String formatTimestamp(java.sql.Timestamp ts) {
        if (ts == null) return null;
        return DT_FORMATTER.format(ts.toInstant());
    }
}

/**
 * Main Streaming CDC Pipeline
 */
public class CdcPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(CdcPipeline.class);

    public static void main(String[] args) {
        LOG.info("════════════════════════════════════════════════════════════════");
        LOG.info("  Starting Streaming CDC Pipeline: MySQL → BigQuery");
        LOG.info("════════════════════════════════════════════════════════════════");

        // Parse options
        CdcPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CdcPipelineOptions.class);

        // Log configuration
        LOG.info("Configuration:");
        LOG.info("  MySQL URL: {}", options.getMysqlJdbcUrl());
        LOG.info("  MySQL Database: {}", options.getMysqlDatabase());
        LOG.info("  MySQL Table: {}", options.getMysqlTable());
        LOG.info("  BigQuery Table: {}", options.getBigQueryTable());
        LOG.info("  Polling Interval: {} seconds", options.getPollingIntervalSeconds());
        LOG.info("  Update All If TS Null: {}", options.getUpdateAllIfTsNull());
        LOG.info("════════════════════════════════════════════════════════════════");

        // Create pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Build JDBC URL
        String jdbcUrl = String.format("%s/%s?useSSL=false&allowPublicKeyRetrieval=true",
                options.getMysqlJdbcUrl(), options.getMysqlDatabase());

        // Define BigQuery schema
        TableSchema bqSchema = new TableSchema()
                .setFields(Arrays.asList(
                        new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("description").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("price").setType("FLOAT").setMode("REQUIRED"),
                        new TableFieldSchema().setName("created_at").setType("DATETIME").setMode("REQUIRED"),
                        new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED")
                ));

        // Generate periodic poll triggers
        PCollection<Long> triggers = pipeline.apply("GeneratePollTriggers",
                GenerateSequence.from(0)
                        .withRate(1, Duration.standardSeconds(options.getPollingIntervalSeconds())));

        // Add key for stateful processing (single key ensures all state is in one place)
        PCollection<KV<String, Long>> keyedTriggers = triggers
                .apply("AddKeyForState", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via(seq -> KV.of("cdc-poller", seq)));

        // Poll MySQL and emit changed records as TableRows
        PCollection<TableRow> tableRows = keyedTriggers
                .apply("StreamingCdcPoll", ParDo.of(new StreamingCdcPollerFn(
                        options.getMysqlJdbcUrl(),
                        options.getMysqlUsername(),
                        options.getMysqlPassword(),
                        options.getMysqlDatabase(),
                        options.getMysqlTable(),
                        options.getUpdateAllIfTsNull())));

        // Write to BigQuery using Storage Write API with CDC (UPSERT semantics)
        // Using STORAGE_API_AT_LEAST_ONCE with RowMutationInformation for proper CDC
        // The target table must have a primary key defined on the 'id' column
        LOG.info("Configuring BigQuery writer:");
        LOG.info("  Method: STORAGE_API_AT_LEAST_ONCE (CDC enabled)");
        LOG.info("  Primary Key: id");
        LOG.info("  Target: {}", options.getBigQueryTable());
        tableRows.apply("WriteToBigQuery",
                BigQueryIO.writeTableRows()
                        .to(options.getBigQueryTable())
                        .withSchema(bqSchema)
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                        .withPrimaryKey(ImmutableList.of("id"))
                        .withRowMutationInformationFn(row -> {
                            // Extract sequence number from the row (stored as _sequence_number)
                            Object seqNum = row.get("_sequence_number");
                            long sequenceNumber = seqNum != null ? ((Number) seqNum).longValue() : System.currentTimeMillis();
                            // Remove the _sequence_number field before writing (it's not part of the schema)
                            row.remove("_sequence_number");
                            return RowMutationInformation.of(
                                    RowMutationInformation.MutationType.UPSERT,
                                    sequenceNumber);
                        })
                        .withCustomGcsTempLocation(
                                org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.of(
                                        options.getGcsTempLocation())));

        // Run the streaming pipeline
        LOG.info("Starting streaming pipeline - will run until cancelled...");
        pipeline.run();
    }
}
