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
package org.apache.arrow.vector.types;

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;

public class SerializedFieldHelper {
  public static SerializedField.Builder getAsBuilder(MajorType type){
    return SerializedField.newBuilder()
            .setMajorType(type);
  }

  public static SerializedField getSerializedField(Field field) {
    SerializedField.Builder serializedFieldBuilder = getAsBuilder(getMajorTypeForField(field));
    if (field.getChildren() != null) {
      for (Field childField : field.getChildren()) {
        serializedFieldBuilder.addChild(getSerializedField(childField));
      }
    }
    serializedFieldBuilder.setNamePart(NamePart.newBuilder().setName(field.getName()));
    return serializedFieldBuilder.build();
  }

  public static Field create(SerializedField serField){
    if (!serField.hasNamePart() || serField.getNamePart().getName().equals("")) {
      throw new RuntimeException();
    }
    return TypeHelper.getFieldForSerializedField(serField);
  }

  public static boolean matches(Field mField, SerializedField field){
    Field f = create(field);
    return f.equals(mField);
  }
}
