/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * Visitor class designed to traversal of a operator tree.  Basis for a number of operator manipulations including fragmentation and materialization.
 * @param <RETURN> The class associated with the return of each visit method.
 * @param <EXTRA> The class object associated with additional data required for a particular operator modification.
 * @param <EXCEP> An optional exception class that can be thrown when a portion of a modification or traversal fails.  Must extend Throwable.  In the case where the visitor does not throw any caught exception, this can be set as RuntimeException.
 */
public interface LogicalVisitor<RETURN, EXTRA, EXCEP extends Throwable> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogicalVisitor.class);


    public RETURN visitScan(Scan scan, EXTRA value) throws EXCEP;
    public RETURN visitStore(Store store, EXTRA value) throws EXCEP;
    public RETURN visitGroupingAggregate(GroupingAggregate groupBy, EXTRA value) throws EXCEP;
    public RETURN visitFilter(Filter filter, EXTRA value) throws EXCEP;
    public RETURN visitFlatten(Flatten flatten, EXTRA value) throws EXCEP;

    public RETURN visitProject(Project project, EXTRA value) throws EXCEP;
    public RETURN visitValues(Values constant, EXTRA value) throws EXCEP;
    public RETURN visitOrder(Order order, EXTRA value) throws EXCEP;
    public RETURN visitJoin(Join join, EXTRA value) throws EXCEP;
    public RETURN visitLimit(Limit limit, EXTRA value) throws EXCEP;
    public RETURN visitRunningAggregate(RunningAggregate runningAggregate, EXTRA value) throws EXCEP;
    public RETURN visitSequence(Sequence sequence, EXTRA value) throws EXCEP;
    public RETURN visitTransform(Transform transform, EXTRA value) throws EXCEP;
    public RETURN visitUnion(Union union, EXTRA value) throws EXCEP;
    public RETURN visitWindow(Window window, EXTRA value) throws EXCEP;
    public RETURN visitWriter(Writer writer, EXTRA value) throws EXCEP;
}
