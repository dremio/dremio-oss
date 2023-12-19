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
package com.dremio.exec.store.iceberg;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.store.RecordWriter;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/**
 * Iceberg manifest writer prel
 */
@Options
public class IcebergManifestWriterPrel extends WriterPrel {
    public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.manifest_writer.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
    public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.manifest_writer.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

    private final CreateTableEntry createTableEntry;
    private final boolean singleWriter;

    public IcebergManifestWriterPrel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            CreateTableEntry createTableEntry,
            boolean singleWriter) {
        super(cluster, traits, child, createTableEntry, child.getRowType());
        this.createTableEntry = createTableEntry;
        this.singleWriter = singleWriter;
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
        PhysicalOperator child = ((Prel) this.getInput()).getPhysicalOperator(creator);
        OpProps props = creator.props(this, creator.getContext().getQueryUserName(), RecordWriter.SCHEMA);
        return new IcebergManifestWriterPOP(
                props,
                child, createTableEntry.getLocation(),
                createTableEntry.getOptions(), createTableEntry.getPlugin(), singleWriter);
    }

    @Override
    public WriterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IcebergManifestWriterPrel(getCluster(), traitSet, sole(inputs), getCreateTableEntry(), singleWriter);
    }
}
