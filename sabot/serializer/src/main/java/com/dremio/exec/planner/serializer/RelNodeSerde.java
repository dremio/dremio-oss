/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.planner.serializer;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTranslatableTable;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.plan.serialization.PRelDataType;
import com.dremio.plan.serialization.PRexNode;
import com.dremio.plan.serialization.PRexWindowBound;
import com.dremio.plan.serialization.PSqlOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;

/**
 * Interface that defines how to move to and from a particular RelNode.
 *
 * @param <REL_NODE> The RelNode implementation.
 * @param <PROTO_NODE> The Message implementation mapped to the RelNode
 */
public interface RelNodeSerde<REL_NODE extends RelNode, PROTO_NODE extends Message> {

  @SuppressWarnings("unchecked")
  default Message serializeGeneric(RelNode node, RelToProto s) {
    return serialize((REL_NODE) node, s);
  }

  /**
   * How to serialize this RelNode
   *
   * @param node The node to serialize
   * @param s Contextual utility to help with serialization.
   * @return The serialized Protobuf message
   */
  PROTO_NODE serialize(REL_NODE node, RelToProto s);

  /**
   * How to deserialize a RelNode from the corresponding Protobuf Message.
   *
   * @param node The Protobuf Message to deserialize
   * @param s Contextual utility used to help with deserialization.
   * @return
   */
  REL_NODE deserialize(PROTO_NODE node, RelFromProto s);

  @SuppressWarnings("unchecked")
  default REL_NODE deserialize(Any any, RelFromProto s) {
    try {
      return deserialize((PROTO_NODE) any.unpack(getDefaultInstance().getClass()), s);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  default PROTO_NODE getDefaultInstance() {
    return (PROTO_NODE)
        MessageTools.getDefaultInstance(MessageTools.getClassArg(this, Message.class, 1));
  }

  @SuppressWarnings("unchecked")
  default Class<REL_NODE> getRelClass() {
    return (Class<REL_NODE>) MessageTools.getClassArg(this, RelNode.class, 0);
  }

  /** Utility to help with RelNode serialization. */
  public interface RelToProto {
    /**
     * Convert a RelNode to a proto
     *
     * @param node The node to serialize
     * @return The identifier for that RelNode within the serialized data.
     */
    int toProto(RelNode node);

    /**
     * Convert a RexNode to a proto
     *
     * @param node The RexNode to convert
     * @return The converted protobuf message.
     */
    PRexNode toProto(RexNode node);

    /**
     * Convert a RelDataType to a proto.
     *
     * @param type The RelDataType to convert.
     * @return The converted protobuf message.
     */
    PRelDataType toProto(RelDataType type);

    /**
     * Converts a SqlOperator to a proto.
     *
     * @param op The SqlOperator to convert.
     * @return The converted protobuf message.
     */
    PSqlOperator toProto(SqlOperator op);

    /**
     * Get the Serde for SqlOperators
     *
     * @return SqlOperatorConverter instance
     */
    SqlOperatorSerde getSqlOperatorSerde();

    /**
     * Converts a rexWindowBound to a proto.
     *
     * @param RexWindowBound The RexWinAggCall to convert.
     * @return The converted protobuf message.
     */
    PRexWindowBound toProto(RexWindowBound rexWindowBound);
  }

  /** Utility to help with RelNode deserialization */
  public interface RelFromProto {

    /**
     * Get the ToRelContext
     *
     * @return The ToRelContext to use when attempting to convert values into RelNodes.
     */
    ToRelContext toRelContext();

    /**
     * Retrieve a RelNode based on its serialized identifier
     *
     * @param index Identifier for a RelNode.
     * @return The associated RelNode.
     */
    RelNode toRel(int index);

    /**
     * Retrieve a RexNode based on its serialized form.
     *
     * @param rex The serialized node.
     * @return The hydrated RexNode.
     */
    RexNode toRex(PRexNode rex);

    /**
     * Retrieve a RexWindowBound based on its serialized form.
     *
     * @param PRexWindowBound The serialized node.
     * @return The hydrated RexNode.
     */
    RexWindowBound toRex(PRexWindowBound pRexWindowBound);

    /**
     * Retrieve a RexNode based on its serialized form and incoming rowType.
     *
     * @param rex The serialized node.
     * @param RelDataType incoming row type.
     * @return The hydrated RexNode.
     */
    RexNode toRex(PRexNode rex, RelDataType rowType);

    /**
     * Retrieve a SqlOperator from its serialized form
     *
     * @param op The serialized operator.
     * @return The deserialized SqlOperator.
     */
    SqlOperator toOp(PSqlOperator op);

    /**
     * Retrieve a RelDataType from it's serialized form.
     *
     * @param type The serialized type.
     * @return The deserialized RelDataType.
     */
    RelDataType toRelDataType(PRelDataType type);

    /**
     * Create a new RelBuilder for purpose of generating RelNodes.
     *
     * @return The new RelBuilder.
     */
    RelBuilder builder();

    /**
     * Get an interface for retrieving tables.
     *
     * @return A TableRetriever.
     */
    TableRetriever tables();

    /**
     * Get an interface for retrieving plugins.
     *
     * @return A PluginRetriever.
     */
    PluginRetriever plugins();

    /**
     * Get the cluster associated with this deserialization.
     *
     * @return A RelOptCluster.
     */
    RelOptCluster cluster();
  }

  /** Simplified interface used for retrieving tables from a context. */
  public interface TableRetriever {

    /**
     * For a particular namespace key, get the associated DremioPrepareTable.
     *
     * @param key Key to lookup
     * @return Table object.
     */
    DremioPrepareTable getTable(NamespaceKey key);

    DremioTranslatableTable getTableSnapshot(CatalogEntityKey catalogEntityKey);
  }

  /** Simplified interface used for retrieving plugin from a context. */
  public interface PluginRetriever {

    /**
     * For a particular namespace key, get the associated DremioPrepareTable.
     *
     * @param name Name to lookup
     * @return StoragePluginId object.
     */
    StoragePlugin getPlugin(String name);
  }
}
