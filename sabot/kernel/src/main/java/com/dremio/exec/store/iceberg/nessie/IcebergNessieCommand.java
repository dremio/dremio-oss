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
import org.apache.iceberg.TableOperations;

import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.nessieapi.ContentsApiGrpc;
import com.dremio.service.nessieapi.TreeApiGrpc;

/**
 * Iceberg Nessie catalog
 */
class IcebergNessieCommand extends IcebergBaseCommand {
    private final IcebergNessieTableIdentifier nessieTableIdentifier;
    private final NessieGrpcClient client;

    public IcebergNessieCommand(IcebergTableIdentifier tableIdentifier,
                                Configuration configuration,
                                ContentsApiGrpc.ContentsApiBlockingStub nessieContentsApiBlockingStub,
                                TreeApiGrpc.TreeApiBlockingStub treeApiBlockingStub,
                                FileSystem fs, OperatorContext context, List<String> dataset) {
        super(configuration, ((IcebergNessieTableIdentifier) tableIdentifier).getTableFolder(), fs, context, dataset);
        nessieTableIdentifier = ((IcebergNessieTableIdentifier) tableIdentifier);
        this.client = new NessieGrpcClient(nessieContentsApiBlockingStub, treeApiBlockingStub);
    }

    @Override
    public TableOperations getTableOperations() {
        return new IcebergNessieTableOperations(client,
                new DremioFileIO(fs, context, dataset, null, null, configuration),
                nessieTableIdentifier);
    }

    public void deleteRootPointerStoreKey () {
      IcebergNessieTableOperations icebergNessieTableOperations = new IcebergNessieTableOperations(client,
        new DremioFileIO(fs, context, dataset, null, null, configuration),
        nessieTableIdentifier);
      icebergNessieTableOperations.deleteKey();
    }
}
