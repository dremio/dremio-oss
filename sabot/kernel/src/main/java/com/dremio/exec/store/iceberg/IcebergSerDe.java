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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.exceptions.RuntimeIOException;

/**
 * Serialization/Deserialization for iceberg entities.
 */
public class IcebergSerDe {

  public static byte[] serializeDataFile(DataFile dataFile) {
    try {
      return serializeToByteArray(dataFile);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to serialize DataFile");
    }
  }

  public static DataFile deserializeDataFile(byte[] serialized) {
    try {
      return (DataFile) deserializeFromByteArray(serialized);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize DataFile");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize DataFile", e);
    }
  }

  private static byte[] serializeToByteArray(Object object) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    }
  }

  private static Object deserializeFromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      ObjectInput in = new ObjectInputStream(bis)) {
      return in.readObject();
    }
  }
}
