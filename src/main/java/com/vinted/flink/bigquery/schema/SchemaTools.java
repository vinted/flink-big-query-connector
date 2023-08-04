package com.vinted.flink.bigquery.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.cloud.bigquery.spi.v2.HttpBigQueryRpc;
import com.google.api.services.bigquery.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaTools implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SchemaTools.class);
    private BigQueryOptions bigQueryOptions;
    private transient BigQuery bigQuery;
    private transient BigQueryRpc bigQueryRpc;

    public BigQuery getBigQuery() {
        if (bigQuery == null) {
            bigQuery = bigQueryOptions.getService();
        }
        return bigQuery;
    }

    public BigQueryRpc getBigQueryRpc() {
        if (bigQueryRpc == null) {
            bigQueryRpc = (HttpBigQueryRpc) bigQueryOptions.getRpc();
        }
        return bigQueryRpc;
    }

    public SchemaTools(BigQueryOptions bigQueryOptions) {
        this.bigQueryOptions = bigQueryOptions;
    }

    public void createTable(TableInfo tableInfo) {
        try {
            getBigQuery().create(tableInfo);
            logger.info("Table {} created successfully", tableInfo.getTableId());
        } catch (BigQueryException e) {
            logger.error("Unable to create table " + tableInfo.getTableId(), e);
            throw e;
        }
    }

    public void updateView(TableId tableId, String query) {
        try {
            var viewMetadata = getBigQuery().getTable(tableId);
            ViewDefinition definition = viewMetadata.getDefinition();
            var updatedViewDefinition = definition.toBuilder().setQuery(query).build();
            getBigQuery().update(viewMetadata.toBuilder().setDefinition(updatedViewDefinition).build());
            logger.info("View query {} updated successfully", tableId);
        } catch (BigQueryException e) {
            logger.error("View query " + tableId + " was not updated.", e);
            throw e;
        }
    }

    public boolean exists(TableId tableId) {
        var table = getBigQuery().getTable(tableId);
        return table != null && table.exists();
    }

    public com.google.cloud.bigquery.Table getTable(TableId tableId, BigQuery.TableOption... bigQueryOptions) {
        return getBigQuery().getTable(tableId, bigQueryOptions);
    }

    public List<Table> listTables(DatasetId datasetId) {
        var data = getBigQuery().listTables(datasetId).iterateAll();
        var list = new ArrayList<Table>();
        data.forEach(list::add);
        return list;
    }

    public List<TableId> listTableIds(DatasetId datasetId) {
        return listTables(datasetId).stream().map(TableInfo::getTableId).collect(Collectors.toList());
    }

    public void updateTable(TableId tableId, Schema schema) {
        var table = getBigQuery().getTable(tableId);
        TableDefinition definition = table.getDefinition();
        var currentBigQueryTableFields = definition.getSchema().getFields();
        var newFields = schema.getFields();
        if (!newFields.containsAll(currentBigQueryTableFields)) {
            var fieldsToRemove = topLevelColumnsToRemove(newFields, currentBigQueryTableFields);
            var newFieldsWithFieldsToRemove = FieldList.of(Stream.concat(newFields.stream(), fieldsToRemove.stream()).collect(Collectors.toList()));
            updateWithRpc(tableId, newFieldsWithFieldsToRemove);
            deleteWithQuery(tableId, fieldsToRemove);
        } else {
            logger.info("No schema changes for {} are needed.", tableId);
        }
    }

    private TableReference getTableReference(TableId tableId) {
        var tableReference = new TableReference();
        tableReference.setTableId(tableId.getTable());
        tableReference.setDatasetId(tableId.getDataset());
        tableReference.setProjectId(tableId.getProject());
        return tableReference;
    }

    private List<TableFieldSchema> fieldsToTableFieldSchema(FieldList fields) {
        return fields.stream().map(field -> {
            var tableFieldSchema = new TableFieldSchema();
            tableFieldSchema.setName(field.getName());
            if (field.getMode() != null) {
                tableFieldSchema.setMode(field.getMode().toString());
            }
            if (field.getType() != null) {
                tableFieldSchema.setType(field.getType().toString());
            }
            tableFieldSchema.setDescription(field.getDescription());
            tableFieldSchema.setPrecision(field.getPrecision());
            tableFieldSchema.setScale(field.getScale());
            tableFieldSchema.setMaxLength(field.getMaxLength());

            if (field.getPolicyTags() != null) {
                tableFieldSchema.setPolicyTags(new TableFieldSchema.PolicyTags().setNames(field.getPolicyTags().getNames()));
            }

            if (field.getType() == LegacySQLTypeName.RECORD) {
                tableFieldSchema.setFields(fieldsToTableFieldSchema(field.getSubFields()));
            }

            return tableFieldSchema;
        }).collect(Collectors.toList());
    }

    private TableSchema getTableSchema(FieldList fields) {
        var tableSchema = new TableSchema();
        return tableSchema.setFields(fieldsToTableFieldSchema(fields));
    }

    private List<Field> topLevelColumnsToRemove(FieldList newFields, FieldList bqTableFields) {
        var newSchemaNames = newFields.stream().map(Field::getName).collect(Collectors.toSet());
        return bqTableFields.stream().filter(f -> !newSchemaNames.contains(f.getName())).collect(Collectors.toList());
    }

    private void updateWithRpc(TableId tableId, FieldList fields) {
        var table = new com.google.api.services.bigquery.model.Table();
        table.setTableReference(getTableReference(tableId));
        table.setSchema(getTableSchema(fields));

        try {
            getBigQueryRpc().patch(table, Map.of());
        } catch (Exception e) {
            logger.error("Unable to alter bigquery schema using rpc", e);
            throw e;
        }
    }

    private Optional<String> buildQuery(TableId tableId, List<Field> removeColumns) {
        var queryStatements = removeColumns.stream().map(f -> String.format("DROP COLUMN IF EXISTS %s", f.getName())).collect(Collectors.toList());
        if (!queryStatements.isEmpty())
            return Optional.of(String.format("ALTER TABLE %s.%s %s", tableId.getDataset(), tableId.getTable(), String.join(",", queryStatements)));
        else {
            return Optional.empty();
        }
    }

    private void deleteWithQuery(TableId tableId, List<Field> columnsToRemove) {
        var combinedQuery = buildQuery(tableId, columnsToRemove);
        if (combinedQuery.isPresent()) {
            try {
                var queryConfig = QueryJobConfiguration.newBuilder(combinedQuery.get()).build();
                getBigQuery().query(queryConfig);
                logger.info("Table {} updated successfully using query", tableId);
            } catch (Exception e) {
                logger.error("Unable to alter bigquery schema using a query", e);
                throw new RuntimeException(e);
            }
        } else {
            logger.debug("Update using query was not performed.");
        }
    }

}
