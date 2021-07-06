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
package com.dremio.exec.store.iceberg.nessie;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.nessieapi.ContentsApiGrpc;
import com.dremio.service.nessieapi.TreeApiGrpc;

/**
 * Iceberg nessie model
 */
public class IcebergNessieModel extends IcebergBaseModel {
    private final ContentsApiGrpc.ContentsApiBlockingStub nessieContentsApiBlockingStub;
    private final TreeApiGrpc.TreeApiBlockingStub treeApiBlockingStub;

    public IcebergNessieModel(String namespace, Configuration configuration,
                              ContentsApiGrpc.ContentsApiBlockingStub nessieContentsApiBlockingStub,
                              TreeApiGrpc.TreeApiBlockingStub treeApiBlockingStub,
                              FileSystem fs, OperatorContext context, List<String> dataset,
                              DatasetCatalogGrpcClient datasetCatalogGrpcClient) {
        super(namespace, configuration, fs, context, dataset, datasetCatalogGrpcClient);
        this.nessieContentsApiBlockingStub = nessieContentsApiBlockingStub;
        this.treeApiBlockingStub = treeApiBlockingStub;
    }

    protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
        return new IcebergNessieCommand(tableIdentifier, this.configuration,
                this.nessieContentsApiBlockingStub, this.treeApiBlockingStub,
                fs, context, dataset);
    }

    @Override
    public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
        return new IcebergNessieTableIdentifier(namespace, rootFolder);
    }
}
