package com.mcp.datalakehouse.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

@Service
public class TrinoToolService {
    private static final Logger logger = LoggerFactory.getLogger(TrinoToolService.class);
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    @Value("${trino.host}")
    private String trinoHost;
    @Value("${trino.port}")
    private int trinoPort;
    @Value("${trino.user}")
    private String trinoUser;

    @Value("${trino.catalog:flink_demo}")
    private String defaultCatalog;
    @Value("${trino.schema:ice_db}")
    private String defaultSchema;

    private Connection getConnection(String catalog) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", trinoUser);

        String url = String.format("jdbc:trino://%s:%d/%s", trinoHost, trinoPort, catalog != null ? catalog : "");
        return DriverManager.getConnection(url, props);
    }

    @Tool(name = "trino_catalogs", description = "List all catalogs available in the Trino cluster.")
    public Object listTrinoCatalogs() {
        try (Connection conn = getConnection(null); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW CATALOGS");
            List<String> catalogs = new ArrayList<>();
            while (rs.next()) catalogs.add(rs.getString(1));
            logger.info("Trino catalogs fetched: {}", catalogs);
            return Map.of("catalogs", catalogs);
        } catch (Exception e) {
            logger.error("Error fetching Trino catalogs: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(name = "trino_schemas", description = "List all schemas in the specified Trino catalog or multiple catalogs.")
    public Object listTrinoSchemas(String... catalogs) {
        List<Map<String, Object>> result = new ArrayList<>();
        // Defensive: if null or empty, use defaultCatalog
        if (catalogs == null || catalogs.length == 0) {
            catalogs = new String[] { defaultCatalog };
        }
        for (String catalog : catalogs) {
            catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
            try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SHOW SCHEMAS FROM " + catalog);
                List<String> schemas = new ArrayList<>();
                while (rs.next()) schemas.add(rs.getString(1));
                logger.info("Trino schemas for catalog '{}' fetched: {}", catalog, schemas);
                result.add(Map.of("catalog", catalog, "schemas", schemas));
            } catch (Exception e) {
                logger.error("Error fetching Trino schemas for catalog '{}': {}", catalog, e.getMessage());
                result.add(Map.of("catalog", catalog, "error", e.getMessage()));
            }
        }
        return result;
    }

    @Tool(name = "trino_iceberg_tables", description = "List all Iceberg tables in the specified Trino catalog and schema.")
    public Object listIcebergTables(String catalog, String schema) {
        catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
        schema = sanitizeIdentifier(schema != null ? schema : defaultSchema);
        try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW TABLES FROM " + catalog + "." + schema);
            List<String> tables = new ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            logger.info("Trino tables for catalog '{}', schema '{}' fetched: {}", catalog, schema, tables);
            return Map.of("tables", tables);
        } catch (Exception e) {
            logger.error("Error fetching Trino tables for catalog '{}', schema '{}': {}", catalog, schema, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(name = "get_iceberg_table_schema", description = "Get the schema of an Iceberg table using Trino.")
    public Object getIcebergTableSchema(String catalog, String schema, String table) {
        if (table == null || table.isEmpty()) {
            return Map.of("error", "Table name must be provided.");
        }
        catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
        schema = sanitizeIdentifier(schema != null ? schema : defaultSchema);
        table = sanitizeIdentifier(table);
        try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("DESCRIBE " + catalog + "." + schema + "." + table);
            List<Map<String, String>> columns = new ArrayList<>();
            while (rs.next()) {
                columns.add(Map.of("name", rs.getString(1), "type", rs.getString(2)));
            }
            logger.info("Trino schema for table '{}' in catalog '{}', schema '{}' fetched: {}", table, catalog, schema, columns);
            return Map.of("columns", columns);
        } catch (Exception e) {
            logger.error("Error fetching Trino schema for table '{}': {}", table, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(name = "execute_trino_query", description = "Execute a SELECT SQL query on Trino and return the result as rows and columns. Only SELECT queries are allowed.")
    public Object executeTrinoQuery(String query, String catalog, String schema) {
        if (query == null || query.trim().isEmpty()) {
            return Map.of("error", "Query must be provided.");
        }
        String lowered = query.trim().toLowerCase();
        if (!lowered.startsWith("select")) {
            return Map.of("error", "Only SELECT queries are allowed.");
        }
        String[] forbidden = {"delete", "update", "drop", "insert", "alter", "truncate"};
        for (String word : forbidden) {
            if (lowered.contains(word)) {
                return Map.of("error", "Query contains forbidden keyword: " + word);
            }
        }
        catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
        schema = sanitizeIdentifier(schema != null ? schema : defaultSchema);
        String prefix = catalog + "." + schema + ".";
        String rewrittenQuery = query;
        if (!query.contains(prefix)) {
            // Simple rewrite for FROM/JOIN clauses
            rewrittenQuery = query.replaceAll("(?i)(FROM|JOIN)\\s+([a-zA-Z_][a-zA-Z0-9_]*)", "$1 " + prefix + "$2");
        }
        try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(rewrittenQuery);
            List<String> columns = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    row.put(columns.get(i - 1), rs.getObject(i));
                }
                rows.add(row);
            }
            logger.info("Trino query executed: {}, rows returned: {}", rewrittenQuery, rows.size());
            return Map.of("columns", columns, "rows", rows, "rewritten_query", rewrittenQuery);
        } catch (Exception e) {
            logger.error("Error executing Trino query: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(name = "iceberg_time_travel_query", description = "\"Execute an Iceberg time travel query using Trino. Specify table, timestamp (ISO 8601 format, e.g. '2024-09-12T15:30:45.123456+05:30') or snapshot_id, and a query. The tool rewrites the query to use FOR TIMESTAMP AS OF or FOR SNAPSHOT AS OF syntax.")
    public Object icebergTimeTravelQuery(String query, String table, String catalog, String schema, String timestamp, String snapshotId) {
        if (query == null || query.trim().isEmpty() || table == null || table.trim().isEmpty()) {
            return Map.of("error", "Both query and table name must be provided.");
        }
        catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
        schema = sanitizeIdentifier(schema != null ? schema : defaultSchema);
        table = sanitizeIdentifier(table);
        String timeTravelClause = "";
        if (timestamp != null && !timestamp.isEmpty()) {
            // Expect ISO 8601, convert to Trino TIMESTAMP literal
            try {
                java.time.OffsetDateTime dt = java.time.OffsetDateTime.parse(timestamp);
                String formatted = dt.toLocalDateTime().toString().replace('T', ' ');
                String offset = dt.getOffset().toString();
                timeTravelClause = " FOR TIMESTAMP AS OF TIMESTAMP '" + formatted + " " + offset + "'";
            } catch (Exception e) {
                return Map.of("error", "Invalid timestamp format: " + timestamp + ". Error: " + e.getMessage());
            }
        } else if (snapshotId != null && !snapshotId.isEmpty()) {
            try {
                long snapId = Long.parseLong(snapshotId);
                timeTravelClause = " FOR VERSION AS OF " + snapId;
            } catch (Exception e) {
                return Map.of("error", "Invalid snapshot_id: " + snapshotId + ". Error: " + e.getMessage());
            }
        }
        String prefix = catalog + "." + schema + "." + table;
        String rewrittenQuery = query.replaceAll("(?i)(FROM|JOIN)\\s+" + table + "(?![\\w.])", "$1 " + prefix + timeTravelClause);
        try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(rewrittenQuery);
            List<String> columns = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    row.put(columns.get(i - 1), rs.getObject(i));
                }
                rows.add(row);
            }
            logger.info("Trino time travel query executed: {}, rows returned: {}", rewrittenQuery, rows.size());
            return Map.of("columns", columns, "rows", rows, "rewritten_query", rewrittenQuery);
        } catch (Exception e) {
            logger.error("Error executing Iceberg time travel query: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(name = "list_iceberg_snapshots", description = "List all snapshots for a given Iceberg table using Trino's $snapshots metadata table.")
    public Object listIcebergSnapshots(String catalog, String schema, String table) {
        if (table == null || table.isEmpty()) {
            return Map.of("error", "Table name must be provided.");
        }
        catalog = sanitizeIdentifier(catalog != null ? catalog : defaultCatalog);
        schema = sanitizeIdentifier(schema != null ? schema : defaultSchema);
        table = sanitizeIdentifier(table);
        String query = "SELECT * FROM " + catalog + "." + schema + ".\"" + table + "$snapshots\"";
        try (Connection conn = getConnection(catalog); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            List<String> columns = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    row.put(columns.get(i - 1), rs.getObject(i));
                }
                rows.add(row);
            }
            // Sort by committed_at descending if present
            if (columns.contains("committed_at")) {
                rows.sort((a, b) -> String.valueOf(b.get("committed_at")).compareTo(String.valueOf(a.get("committed_at"))));
            }
            // Label latest/oldest
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) rows.get(i).put("label", "latest");
                else if (i == rows.size() - 1) rows.get(i).put("label", "oldest");
            }
            logger.info("Trino snapshots for table '{}' in catalog '{}', schema '{}' fetched: {}", table, catalog, schema, rows.size());
            return Map.of("columns", columns, "rows", rows);
        } catch (Exception e) {
            logger.error("Error fetching Iceberg snapshots for table '{}': {}", table, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    private String sanitizeIdentifier(String identifier) {
        if (identifier == null || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + identifier);
        }
        return identifier;
    }
}
