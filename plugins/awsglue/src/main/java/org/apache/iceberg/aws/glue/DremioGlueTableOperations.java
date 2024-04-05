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
package org.apache.iceberg.aws.glue;

import java.util.Map;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.services.glue.GlueClient;

/** Glue table operations */
public class DremioGlueTableOperations extends GlueTableOperations {

  private final FileIO dremioFileIO;

  public DremioGlueTableOperations(
      GlueClient glue,
      LockManager lockManager,
      String catalogName,
      Map<String, String> properties,
      FileIO fileIO,
      TableIdentifier tableIdentifier) {
    super(
        glue,
        lockManager,
        catalogName,
        new AwsProperties(properties),
        properties,
        null,
        tableIdentifier);
    this.dremioFileIO = fileIO;
  }

  @Override
  public FileIO io() {
    // After https://github.com/apache/iceberg/pull/5756,
    // `FileIO` is constructed in the parent class using catalog properties and hadoop conf.
    // `DremioFileIO` cannot be constructed without a `Plugin` in the parent class.
    // Hence, by default construct `S3FileIO` (as io-impl is not configured)
    // and override it with `DremioFileIO`.
    return dremioFileIO;
  }
}
