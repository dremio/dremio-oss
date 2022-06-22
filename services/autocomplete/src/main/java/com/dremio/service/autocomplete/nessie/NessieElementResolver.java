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
package com.dremio.service.autocomplete.nessie;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.tokens.DremioToken;

/**
 * Resolves the proper Nessie elements given the context.
 */
public final class NessieElementResolver {
  private final NessieElementReader reader;

  public NessieElementResolver(NessieElementReader reader) {
    Preconditions.checkNotNull(reader);

    this.reader = reader;
  }

  public List<? extends NessieElement> resolve(DremioToken dremioToken) {
    Optional<NessieElementType> optionalNessieElementType = NessieElementType.tryConvertFromDremioToken(dremioToken);
    NessieElementType nessieElementType = optionalNessieElementType.get();
    switch (nessieElementType)
    {
    case BRANCH:
      return this.reader.getBranches();

    case COMMIT:
      return this.reader.getCommits();

    case TAG:
      return this.reader.getTags();

    default:
      throw new RuntimeException("UNKNOWN NESSIE ELEMENT TYPE: " + nessieElementType);
    }
  }
}
