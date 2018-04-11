/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.provision.service;

import com.dremio.provision.DistroType;
import com.google.common.base.Objects;

/**
 * Class to create a Key to get Yarn defaults per distro/security combination
 */
public class DistroSecurityKey {
  private final DistroType dType;
  private final boolean isSecurityOn;

  public DistroSecurityKey(DistroType dType, boolean isSecurityOn) {
    this.dType = dType;
    this.isSecurityOn = isSecurityOn;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dType, isSecurityOn);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DistroSecurityKey)) {
      return false;
    }

    DistroSecurityKey dOther = (DistroSecurityKey) other;
    if (dType == dOther.dType && isSecurityOn == dOther.isSecurityOn) {
      return true;
    }
    return false;
  }
}
