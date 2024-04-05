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
package com.dremio.exec.physical.base;

import com.dremio.exec.physical.config.AbstractSort;
import com.dremio.exec.physical.config.AbstractTableFunctionPOP;
import com.dremio.exec.physical.config.BridgeExchange;
import com.dremio.exec.physical.config.BridgeFileReader;
import com.dremio.exec.physical.config.BroadcastSender;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.physical.config.FlattenPOP;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.exec.physical.config.Limit;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.exec.physical.config.MergingReceiverPOP;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.physical.config.RoundRobinSender;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.physical.config.SingleSender;
import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.physical.config.UnionExchange;
import com.dremio.exec.physical.config.UnorderedReceiver;
import com.dremio.exec.physical.config.Values;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP;

public abstract class AbstractPhysicalVisitor<T, X, E extends Throwable>
    implements PhysicalVisitor<T, X, E> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractPhysicalVisitor.class);

  @Override
  public T visitExchange(Exchange exchange, X value) throws E {
    return visitOp(exchange, value);
  }

  @Override
  public T visitWriterCommiter(WriterCommitterPOP commiter, X value) throws E {
    return visitOp(commiter, value);
  }

  @Override
  public T visitUnion(UnionAll union, X value) throws E {
    return visitOp(union, value);
  }

  @Override
  public T visitWriter(Writer writer, X value) throws E {
    return visitOp(writer, value);
  }

  @Override
  public T visitFilter(Filter filter, X value) throws E {
    return visitOp(filter, value);
  }

  @Override
  public T visitWindowFrame(WindowPOP windowFrame, X value) throws E {
    return visitOp(windowFrame, value);
  }

  @Override
  public T visitProject(Project project, X value) throws E {
    return visitOp(project, value);
  }

  @Override
  public T visitEmptyValues(EmptyValues op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitSort(AbstractSort sort, X value) throws E {
    return visitOp(sort, value);
  }

  @Override
  public T visitLimit(Limit limit, X value) throws E {
    return visitOp(limit, value);
  }

  @Override
  public T visitStreamingAggregate(StreamingAggregate agg, X value) throws E {
    return visitOp(agg, value);
  }

  @Override
  public T visitHashAggregate(HashAggregate agg, X value) throws E {
    return visitOp(agg, value);
  }

  @Override
  public T visitSender(Sender sender, X value) throws E {
    return visitOp(sender, value);
  }

  @Override
  public T visitFlatten(FlattenPOP flatten, X value) throws E {
    return visitOp(flatten, value);
  }

  @Override
  public T visitReceiver(Receiver receiver, X value) throws E {
    return visitOp(receiver, value);
  }

  @Override
  public T visitGroupScan(GroupScan groupScan, X value) throws E {
    return visitOp(groupScan, value);
  }

  @Override
  public T visitSubScan(SubScan subScan, X value) throws E {
    return visitOp(subScan, value);
  }

  @Override
  public T visitStore(Store store, X value) throws E {
    return visitOp(store, value);
  }

  public T visitChildren(PhysicalOperator op, X value) throws E {
    for (PhysicalOperator child : op) {
      child.accept(this, value);
    }
    return null;
  }

  @Override
  public T visitMergeJoin(MergeJoinPOP join, X value) throws E {
    return visitOp(join, value);
  }

  @Override
  public T visitHashJoin(HashJoinPOP join, X value) throws E {
    return visitOp(join, value);
  }

  @Override
  public T visitNestedLoopJoin(NestedLoopJoinPOP join, X value) throws E {
    return visitOp(join, value);
  }

  @Override
  public T visitHashPartitionSender(HashPartitionSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitUnorderedReceiver(UnorderedReceiver op, X value) throws E {
    return visitReceiver(op, value);
  }

  @Override
  public T visitMergingReceiver(MergingReceiverPOP op, X value) throws E {
    return visitReceiver(op, value);
  }

  @Override
  public T visitHashPartitionSender(HashToRandomExchange op, X value) throws E {
    return visitExchange(op, value);
  }

  @Override
  public T visitBroadcastSender(BroadcastSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitRoundRobinSender(RoundRobinSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitScreen(Screen op, X value) throws E {
    return visitStore(op, value);
  }

  @Override
  public T visitSingleSender(SingleSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitUnionExchange(UnionExchange op, X value) throws E {
    return visitExchange(op, value);
  }

  @Override
  public T visitBridgeExchange(BridgeExchange op, X value) throws E {
    return visitExchange(op, value);
  }

  @Override
  public T visitValues(Values op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitConvertFromJson(ConvertFromJsonPOP op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitTableFunction(AbstractTableFunctionPOP op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitBridgeFileReader(BridgeFileReader op, X value) throws E {
    return visitSubScan(op, value);
  }

  @Override
  public T visitOp(PhysicalOperator op, X value) throws E {
    throw new UnsupportedOperationException(
        String.format(
            "The PhysicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.",
            this.getClass().getCanonicalName(), op.getClass().getCanonicalName()));
  }
}
