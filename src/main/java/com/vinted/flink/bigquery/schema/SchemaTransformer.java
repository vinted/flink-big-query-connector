package com.vinted.flink.bigquery.schema;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.common.collect.ImmutableMap;

import java.util.stream.IntStream;

public class SchemaTransformer {
    private static ImmutableMap<Field.Mode, TableFieldSchema.Mode> BQTableSchemaModeMap = ImmutableMap.of(
            Field.Mode.NULLABLE,
            TableFieldSchema.Mode.NULLABLE,
            Field.Mode.REPEATED,
            TableFieldSchema.Mode.REPEATED,
            Field.Mode.REQUIRED,
            TableFieldSchema.Mode.REQUIRED
    );

    private static ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> BQTableSchemaTypeMap = new ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
            .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
            .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
            .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
            .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
            .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
            .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
            .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
            .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
            .put(StandardSQLTypeName.BIGNUMERIC, TableFieldSchema.Type.BIGNUMERIC)
            .put(StandardSQLTypeName.JSON, TableFieldSchema.Type.JSON)
            .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
            .put(StandardSQLTypeName.INTERVAL, TableFieldSchema.Type.INTERVAL)
            .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
            .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
            .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
            .build();

    /**
     * Converts from BigQuery client Table Schema to bigquery storage API Table Schema.
     *
     * @param schema the BigQuery client Table Schema
     * @return the bigquery storage API Table Schema
     */
    public static TableSchema convertTableSchema(Schema schema) {
        var result = TableSchema.newBuilder();

        IntStream.range(0, schema.getFields().size()).forEach(i -> {
            result.addFields(i, convertFieldSchema(schema.getFields().get(i)));
        });

        return result.build();
    }

    /**
     * Converts from bigquery v2 Field Schema to bigquery storage API Field Schema.
     *
     * @param field the BigQuery client Field Schema
     * @return the bigquery storage API Field Schema
     */
    public static TableFieldSchema convertFieldSchema(Field field) {
        var fieldVar = field;
        var result = TableFieldSchema.newBuilder();
        if (field.getMode() == null) {
            fieldVar = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
        }
        result.setMode(BQTableSchemaModeMap.get(fieldVar.getMode()));
        result.setName(field.getName());
        result.setType(BQTableSchemaTypeMap.get(fieldVar.getType().getStandardType()));
        if (fieldVar.getDescription() != null) {
            result.setDescription(fieldVar.getDescription());
        }
        if (fieldVar.getSubFields() != null) {
            Field finalFieldVar = fieldVar;
            IntStream.range(0, fieldVar.getSubFields().size()).forEach(i -> {
                result.addFields(i, convertFieldSchema(finalFieldVar.getSubFields().get(i)));
            });
        }
        return result.build();
    }
}
