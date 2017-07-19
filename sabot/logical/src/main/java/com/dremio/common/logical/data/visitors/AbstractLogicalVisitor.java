/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.common.logical.data.visitors;

import com.dremio.common.logical.data.Filter;
import com.dremio.common.logical.data.Flatten;
import com.dremio.common.logical.data.GroupingAggregate;
import com.dremio.common.logical.data.Join;
import com.dremio.common.logical.data.Limit;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.Order;
import com.dremio.common.logical.data.Project;
import com.dremio.common.logical.data.RunningAggregate;
import com.dremio.common.logical.data.Scan;
import com.dremio.common.logical.data.Sequence;
import com.dremio.common.logical.data.Store;
import com.dremio.common.logical.data.Transform;
import com.dremio.common.logical.data.Union;
import com.dremio.common.logical.data.Values;
import com.dremio.common.logical.data.Window;
import com.dremio.common.logical.data.Writer;


public abstract class AbstractLogicalVisitor<T, X, E extends Throwable> implements LogicalVisitor<T, X, E> {

    public T visitOp(LogicalOperator op, X value) throws E{
        throw new UnsupportedOperationException(String.format(
                "The LogicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.", this
                .getClass().getCanonicalName(), op.getClass().getCanonicalName()));
    }

    @Override
    public T visitScan(Scan scan, X value) throws E {
        return visitOp(scan, value);
    }

    @Override
    public T visitStore(Store store, X value) throws E {
        return visitOp(store, value);
    }

    @Override
    public T visitFilter(Filter filter, X value) throws E {
        return visitOp(filter, value);
    }

    @Override
    public T visitFlatten(Flatten flatten, X value) throws E {
        return visitOp(flatten, value);
    }

    @Override
    public T visitProject(Project project, X value) throws E {
        return visitOp(project, value);
    }

    @Override
    public T visitOrder(Order order, X value) throws E {
        return visitOp(order, value);
    }

    @Override
    public T visitJoin(Join join, X value) throws E {
        return visitOp(join, value);
    }

    @Override
    public T visitLimit(Limit limit, X value) throws E {
        return visitOp(limit, value);
    }

    @Override
    public T visitRunningAggregate(RunningAggregate runningAggregate, X value) throws E {
        return visitOp(runningAggregate, value);
    }

    @Override
    public T visitGroupingAggregate(GroupingAggregate groupBy, X value) throws E {
      return visitOp(groupBy, value);
    }

    @Override
    public T visitSequence(Sequence sequence, X value) throws E {
        return visitOp(sequence, value);
    }

    @Override
    public T visitTransform(Transform transform, X value) throws E {
        return visitOp(transform, value);
    }

    @Override
    public T visitUnion(Union union, X value) throws E {
        return visitOp(union, value);
    }

    @Override
    public T visitWindow(Window window, X value) throws E {
        return visitOp(window, value);
    }

    @Override
    public T visitValues(Values constant, X value) throws E {
       return visitOp(constant, value);
    }

    @Override
    public T visitWriter(Writer writer, X value) throws E {
      return visitOp(writer, value);
    }
}
