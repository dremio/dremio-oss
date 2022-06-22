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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.protostuff.ByteString;

public class IcebergExtendedProp implements Serializable {

    private final ByteString partitionSpecs;
    private final byte[] icebergExpression;
    private final long snapshotId;
    private final String icebergSchema;

    @JsonCreator
    public IcebergExtendedProp(
        @JsonProperty("partitionSpecs") ByteString partitionSpecs,
        @JsonProperty("icebergExpression") byte[] icebergExpression,
        @JsonProperty(value = "snapshotId", defaultValue = "-1") long snapshotId,
        @JsonProperty("icebergSchema") String icebergSchema
    ) {
        this.partitionSpecs = partitionSpecs;
        this.icebergExpression = icebergExpression;
        this.snapshotId = snapshotId;
        this.icebergSchema = icebergSchema;
    }

    public ByteString getPartitionSpecs() {
        return partitionSpecs;
    }

    public byte[] getIcebergExpression() {
        return icebergExpression;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public String getIcebergSchema() {
        return icebergSchema;
    }
}
