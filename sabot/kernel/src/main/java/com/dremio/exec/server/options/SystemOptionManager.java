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
package com.dremio.exec.server.options;

import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProto;
import com.dremio.options.impl.BaseOptionManager;
import com.dremio.service.Service;
import java.util.List;

/**
 * {@link OptionManager} that holds options. Only one instance of this class exists per node.
 * Options set at the system level affect the entire system and persist between restarts.
 */
public abstract class SystemOptionManager extends BaseOptionManager
    implements Service, ProjectOptionManager {
  public static final String SYSTEM_OPTION_PREFIX = "dremio.debug.sysopt.";

  protected SystemOptionManager(OptionValidatorListing optionValidatorListing) {
    super(optionValidatorListing);
  }

  @Override
  protected abstract boolean supportsOptionType(OptionValue.OptionType type);

  public abstract void populateCache();

  public abstract void clearCachedOptionProtoList();

  public abstract List<OptionValueProto> getOptionProtoListFromStore();

  @Override
  public abstract void start() throws Exception;

  @Override
  public abstract void close() throws Exception;
}
