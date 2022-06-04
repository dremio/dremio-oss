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
package com.dremio.service.nessie.upgrade.version040.model;

public class NessieCommit {
  private Hash hash;
  private Hash ancestor;
  private Metadata metadata;
  private Operations operations;

  public Hash getHash() {
    return hash;
  }

  public void setHash(Hash hash) {
    this.hash = hash;
  }

  public Hash getAncestor() {
    return ancestor;
  }

  public void setAncestor(Hash ancestor) {
    this.ancestor = ancestor;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public Operations getOperations() {
    return operations;
  }

  public void setOperations(Operations operations) {
    this.operations = operations;
  }
}
