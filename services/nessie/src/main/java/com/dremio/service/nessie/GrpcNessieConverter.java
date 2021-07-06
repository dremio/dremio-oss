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
package com.dremio.service.nessie;

import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableUnchanged;
import org.projectnessie.model.Operation;

import com.dremio.service.nessieapi.Branch;
import com.dremio.service.nessieapi.Hash;
import com.dremio.service.nessieapi.NessieConfiguration;
import com.dremio.service.nessieapi.Reference;
import com.dremio.service.nessieapi.Tag;

/**
 * Helper methods for converting Nessie objects to and from gRPC objects.
 */
class GrpcNessieConverter {
  static Reference toGrpc(org.projectnessie.model.Reference reference) {
    final Reference.Builder refBuilder = Reference.newBuilder();

    if (reference instanceof org.projectnessie.model.Branch) {
      refBuilder.setBranch(Branch.newBuilder().setName(reference.getName()).setHash(reference.getHash()));
    } else if (reference instanceof org.projectnessie.model.Tag) {
      refBuilder.setTag(Tag.newBuilder().setName(reference.getName()).setHash(reference.getHash()));
    } else if (reference instanceof org.projectnessie.model.Hash) {
      refBuilder.setHash(Hash.newBuilder().setHash(reference.getHash()));
    }

    return refBuilder.build();
  }

  static Branch toGrpc(org.projectnessie.model.Branch branch) {
    final Branch.Builder branchBuilder = Branch.newBuilder();
    branchBuilder.setName(branch.getName()).setHash(branch.getHash());
    return branchBuilder.build();
  }

  static NessieConfiguration toGrpc(org.projectnessie.model.NessieConfiguration nessieConfiguration) {
    final NessieConfiguration.Builder nessieConfigurationBuilder = NessieConfiguration.newBuilder();
    nessieConfigurationBuilder.setDefaultBranch(nessieConfiguration.getDefaultBranch()).setVersion(nessieConfiguration.getVersion());
    return nessieConfigurationBuilder.build();
  }

  static com.dremio.service.nessieapi.Contents toGrpc(org.projectnessie.model.Contents contents) {
    if (contents instanceof IcebergTable) {
      return com.dremio.service.nessieapi.Contents
          .newBuilder()
          .setType(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE)
          .setIcebergTable(com.dremio.service.nessieapi.Contents.IcebergTable
              .newBuilder()
              .setMetadataLocation(((IcebergTable) contents).getMetadataLocation()))
          .build();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  static org.projectnessie.model.Reference fromGrpc(com.dremio.service.nessieapi.Reference reference) {
    if (reference.hasBranch()) {
      return org.projectnessie.model.Branch.of(reference.getBranch().getName(), reference.getBranch().getHash());
    } else if (reference.hasTag()) {
      return org.projectnessie.model.Tag.of(reference.getTag().getName(), reference.getTag().getHash());
    } else if (reference.hasHash()) {
      return org.projectnessie.model.Hash.of(reference.getHash().getHash());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  static ContentsKey fromGrpc(com.dremio.service.nessieapi.ContentsKey contentsKey) {
    return ContentsKey.of(contentsKey.getElementsList());
  }

  static Contents fromGrpc(com.dremio.service.nessieapi.Contents contents) {
    switch (contents.getType()) {
      case ICEBERG_TABLE:
        return IcebergTable.of(contents.getIcebergTable().getMetadataLocation());
      default:
        throw new UnsupportedOperationException();
    }
  }

  static Operation fromGrpc(com.dremio.service.nessieapi.Operation operation) {
    switch (operation.getType()) {
      case PUT:
        return ImmutablePut.builder().key(fromGrpc(operation.getContentsKey())).contents(fromGrpc(operation.getContents())).build();
      case DELETE:
        return ImmutableDelete.builder().key(fromGrpc(operation.getContentsKey())).build();
      case UNCHANGED:
        return ImmutableUnchanged.builder().key(fromGrpc(operation.getContentsKey())).build();
      default:
        throw new UnsupportedOperationException();
    }
  }
}
