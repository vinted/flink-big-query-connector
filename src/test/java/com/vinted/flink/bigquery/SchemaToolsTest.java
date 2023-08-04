package com.vinted.flink.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.spi.v2.HttpBigQueryRpc;
import com.vinted.flink.bigquery.schema.SchemaTools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SchemaToolsTest {
    @Mock
    BigQuery aBqClient;
    @Mock
    HttpBigQueryRpc aBqRpcClient;

    @Mock
    BigQueryOptions aBqOptions;
    @Mock
    Table aTable;
    @Mock
    TableDefinition aTableDefinition;

    TableId tableId = TableId.of("test-dwh-poc", "cdc", "test1");

    @BeforeEach
    public void init() {
        Mockito.reset(aBqClient);
        Mockito.reset(aBqRpcClient);
        Mockito.reset(aBqOptions);
        Mockito.reset(aTable);
        Mockito.reset(aTableDefinition);

        lenient().doReturn(aBqClient).when(aBqOptions).getService();
        lenient().doReturn(aBqRpcClient).when(aBqOptions).getRpc();
        lenient().doReturn(aTable).when(aBqClient).getTable(any(TableId.class), any());
        lenient().doReturn(aTableDefinition).when(aTable).getDefinition();
    }

    @Test
    public void shouldAddAndRemoveFields() throws InterruptedException {
        var bqSchema = Schema.of(FieldList.of(Field.of("a1", LegacySQLTypeName.STRING)));
        doReturn(bqSchema).when(aTableDefinition).getSchema();
        var schemaTools = new SchemaTools(aBqOptions);

        var newSchema = Schema.of(
                FieldList.of(
                        Field.of("b1", LegacySQLTypeName.INTEGER),
                        Field.of("b2", LegacySQLTypeName.RECORD, FieldList.of(Field.of("c1", LegacySQLTypeName.STRING)))
                ));

        var bqTableFieldSchema = List.of(
                new TableFieldSchema().setName("a1").setType(LegacySQLTypeName.STRING.toString()),
                new TableFieldSchema().setName("b1").setType(LegacySQLTypeName.INTEGER.toString()),
                new TableFieldSchema().setName("b2").setType(LegacySQLTypeName.RECORD.toString())
                        .setFields(List.of(new TableFieldSchema().setName("c1").setType(LegacySQLTypeName.STRING.toString())))
        );

        var bqClientCaptor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        var bqRpcClientCaptor = ArgumentCaptor.forClass(com.google.api.services.bigquery.model.Table.class);
        schemaTools.updateTable(tableId, newSchema);

        verify(aBqClient, times(1)).query(bqClientCaptor.capture());
        verify(aBqRpcClient, times(1)).patch(bqRpcClientCaptor.capture(), anyMap());
        assertThat(bqClientCaptor.getValue().getQuery()).isEqualTo("ALTER TABLE cdc.test1 DROP COLUMN IF EXISTS a1");
        assertThat(bqRpcClientCaptor.getValue().getSchema().getFields()).containsExactlyInAnyOrderElementsOf(bqTableFieldSchema);
    }

    @Test
    public void shouldCreateNewTable() throws InterruptedException {
        var bqSchema = Schema.of();
        lenient().doReturn(bqSchema).when(aTableDefinition).getSchema();
        var schemaTools = new SchemaTools(aBqOptions);

        var newSchema = Schema.of(FieldList.of(Field.of("b1", StandardSQLTypeName.INT64)));

        var captor = ArgumentCaptor.forClass(TableInfo.class);
        schemaTools.createTable(TableInfo.newBuilder(tableId, StandardTableDefinition.of(newSchema)).build());

        verify(aBqClient, times(1)).create(captor.capture());
        assertThat(captor.getValue().getDefinition().getSchema().getFields()).containsExactlyInAnyOrderElementsOf(newSchema.getFields());

    }

    @Test
    public void shouldDoNothingWhenSchemasAreTheSame() throws InterruptedException {
        var bqSchema = Schema.of(
                FieldList.of(
                        Field.of("b1", LegacySQLTypeName.INTEGER),
                        Field.of("a1", LegacySQLTypeName.STRING)
                )
        );
        var newSchema = Schema.of(
                FieldList.of(
                        Field.of("a1", LegacySQLTypeName.STRING),
                        Field.of("b1", LegacySQLTypeName.INTEGER)
                        )
        );

        lenient().doReturn(bqSchema).when(aTableDefinition).getSchema();
        var schemaTools = new SchemaTools(aBqOptions);

        var bqClientCaptor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        var bqRpcClientCaptor = ArgumentCaptor.forClass(com.google.api.services.bigquery.model.Table.class);
        schemaTools.updateTable(tableId, newSchema);

        verify(aBqClient, times(0)).query(bqClientCaptor.capture());
        verify(aBqRpcClient, times(0)).patch(bqRpcClientCaptor.capture(), anyMap());
    }
}
