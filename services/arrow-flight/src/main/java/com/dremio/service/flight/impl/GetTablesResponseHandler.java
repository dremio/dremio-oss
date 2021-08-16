package com.dremio.service.flight.impl;

import static org.apache.arrow.flight.FlightProducer.*;

import java.util.stream.IntStream;

import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;

/**
 * The UserResponseHandler that consumes a GetTablesResp.
 */
public class GetTablesResponseHandler implements UserResponseHandler {
  private final BufferAllocator allocator;
  private final ServerStreamListener listener;

  public GetTablesResponseHandler(BufferAllocator allocator, ServerStreamListener listener) {
    this.allocator = allocator;
    this.listener = listener;
  }

  @Override
  public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
                       QueryWritableBatch result) {

  }

  @Override
  public void completed(UserResult result) {
    final UserProtos.GetTablesResp getTablesResp = result.unwrap(UserProtos.GetTablesResp.class);
    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("schema_name");
      VarCharVector tableNameVector = (VarCharVector) vectorSchemaRoot.getVector("table_name");
      VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");

      final int tablesCount = getTablesResp.getTablesCount();
      final IntStream range = IntStream.range(1, tablesCount);

      range.forEach(i ->{
        final UserProtos.TableMetadata tables = getTablesResp.getTables(i);
        catalogNameVector.setSafe(i, new Text(tables.getCatalogName()));
        schemaNameVector.setSafe(i, new Text(tables.getSchemaName()));
        tableNameVector.setSafe(i, new Text(tables.getTableName()));
        tableTypeVector.setSafe(i, new Text(tables.getType()));
      });

      vectorSchemaRoot.setRowCount(tablesCount);
      listener.putNext();
      listener.completed();
    }
  }
}
