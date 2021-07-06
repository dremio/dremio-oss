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
package com.dremio.exec.store.iceberg.manifestwriter;

import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Iceberg is disabled, overrides ensure no operations done
 */
public class NoOpIcebergCommitOpHelper extends IcebergCommitOpHelper {

    public NoOpIcebergCommitOpHelper(OperatorContext context, WriterCommitterPOP config) {
        super(context, config);
    }

    @Override
    public void setup(VectorAccessible incoming) {
    }

    @Override
    public void consumeData(int records) throws Exception {
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void close() {
    }
}