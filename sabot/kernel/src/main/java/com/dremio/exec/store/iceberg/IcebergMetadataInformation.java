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

/**
 * Holds Iceberg metadata file information and type of metadata file
 */
public class IcebergMetadataInformation implements Serializable {

  public enum IcebergMetadataFileType {
    ADD_DATAFILE,
    DELETE_DATAFILE,
    MANIFEST_FILE;
  }

  private final byte[] icebergMetadataFileByte;
  private final IcebergMetadataFileType icebergMetadataFileType;

  public IcebergMetadataInformation(byte[] icebergMetadataFileByte, IcebergMetadataFileType icebergMetadataFileType) {
    this.icebergMetadataFileByte = icebergMetadataFileByte;
    this.icebergMetadataFileType = icebergMetadataFileType;
  }


  public byte[] getIcebergMetadataFileByte() {
    return icebergMetadataFileByte;
  }

  public IcebergMetadataFileType getIcebergMetadataFileType() {
    return icebergMetadataFileType;
  }
}
