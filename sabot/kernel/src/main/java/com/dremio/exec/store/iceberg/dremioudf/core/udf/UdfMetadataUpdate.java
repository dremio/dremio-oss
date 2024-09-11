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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import org.apache.iceberg.MetadataUpdate;

public interface UdfMetadataUpdate extends MetadataUpdate {
  default void applyTo(UdfMetadata.Builder metadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a UDF", this.getClass().getSimpleName()));
  }

  class AddUdfVersion implements UdfMetadataUpdate {
    private final UdfVersion udfVersion;

    public AddUdfVersion(UdfVersion udfVersion) {
      this.udfVersion = udfVersion;
    }

    public UdfVersion udfVersion() {
      return udfVersion;
    }

    @Override
    public void applyTo(UdfMetadata.Builder udfMetadataBuilder) {
      udfMetadataBuilder.addVersion(udfVersion);
    }
  }

  class AddSignature implements UdfMetadataUpdate {
    private final UdfSignature signature;

    public AddSignature(UdfSignature signature) {
      this.signature = signature;
    }

    public UdfSignature signature() {
      return signature;
    }

    @Override
    public void applyTo(UdfMetadata.Builder udfMetadataBuilder) {
      udfMetadataBuilder.addSignature(signature);
    }
  }

  class SetCurrentUdfVersion implements UdfMetadataUpdate {
    private final String versionId;

    public SetCurrentUdfVersion(String versionId) {
      this.versionId = versionId;
    }

    public String versionId() {
      return versionId;
    }

    @Override
    public void applyTo(UdfMetadata.Builder udfMetadataBuilder) {
      udfMetadataBuilder.setCurrentVersionId(versionId);
    }
  }
}
