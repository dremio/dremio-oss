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
package com.dremio.exec.record;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.map.CaseInsensitiveMap;

/**
 * Supports efficient field search in batch schema
 */
public final class SearchableBatchSchema extends BatchSchema{
    private final Map<String, Field> fieldMap;

    private SearchableBatchSchema(List<Field> fields) {
        super(fields);
        fieldMap = CaseInsensitiveMap.<Field>newHashMap();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
    }

    @Override
    public java.util.Optional<Field> findFieldIgnoreCase(String fieldName) {
        return java.util.Optional.ofNullable(fieldMap.get(fieldName));
    }

    public static SearchableBatchSchema of(BatchSchema schema) {
        return new SearchableBatchSchema(schema.getFields());
    }
}
