/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.datastore.indexed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


/**
 * Provide mapping of to field short names to index keys.
 */
public class FilterIndexMapping {

  private final ImmutableMap<String, IndexKey> shortToKey;
  private final ImmutableList<String> containsKeys;

  public FilterIndexMapping(IndexKey...fields) {
    ImmutableMap.Builder<String, IndexKey> shortToKeyB = ImmutableMap.builder();
    ImmutableList.Builder<String> containsKeysB = ImmutableList.builder();
    for(IndexKey f : fields){
      shortToKeyB.put(f.getShortName(), f);
      if(f.isIncludeInSearchAllFields()){
        containsKeysB.add(f.getIndexFieldName());
      }
    }
    this.shortToKey = shortToKeyB.build();
    this.containsKeys = containsKeysB.build();
  }

  public IndexKey getKey(final String shortName){
    return shortToKey.get(shortName);
  }

  public ImmutableList<String> getSearchAllIndexKeys(){
    return containsKeys;
  }

}
