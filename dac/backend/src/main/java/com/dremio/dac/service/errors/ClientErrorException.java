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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.common.ValidationErrorMessages;

/**
 * 4xx errors
 *
 */
public class ClientErrorException extends DACRuntimeException {
  private static final long serialVersionUID = 1L;

  private final ValidationErrorMessages messages;

  public ClientErrorException(String message, Throwable cause) {
    super(message, cause);
    this.messages = null;
  }

  public ClientErrorException(String message, ValidationErrorMessages messages) {
    super(message);
    this.messages = messages;
  }

  public ClientErrorException(String message) {
    super(message);
    this.messages = null;
  }

  public ClientErrorException(Throwable cause) {
    super(cause);
    this.messages = null;
  }

  public ValidationErrorMessages getMessages() {
    return messages;
  }

}
