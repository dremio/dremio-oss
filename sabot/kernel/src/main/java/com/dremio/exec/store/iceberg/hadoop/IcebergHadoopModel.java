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
package com.dremio.exec.store.iceberg.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Entry point for Hadoop based Iceberg tables
 */
public class IcebergHadoopModel extends IcebergBaseModel {

    public IcebergHadoopModel(Configuration configuration) {
        this(EMPTY_NAMESPACE, configuration, null, null, null, null);
    }

    public IcebergHadoopModel(String namespace, Configuration configuration,
                              FileSystem fs, OperatorContext context, List<String> dataset,
                              DatasetCatalogGrpcClient datasetCatalogGrpcClient) {
        super(namespace, configuration, fs, context, dataset, datasetCatalogGrpcClient);
    }

    protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
        return new IcebergHadoopCommand(tableIdentifier, configuration, fs, context, dataset);
    }

    @Override
    public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
        return new IcebergHadoopTableIdentifier(namespace, rootFolder);
    }
}
