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
package com.dremio.exec.exception;

public class SchemaChangeException extends RuntimeException{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaChangeException.class);

  public SchemaChangeException() {
    super();
  }

  public SchemaChangeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public SchemaChangeException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaChangeException(String message) {
    super(message);
  }

  public SchemaChangeException(Throwable cause) {
    super(cause);
  }

  public SchemaChangeException(String message, Object...objects){
    super(String.format(message, objects));
  }

  public SchemaChangeException(String message, Throwable cause, Object...objects){
    super(String.format(message, objects), cause);
  }
}
