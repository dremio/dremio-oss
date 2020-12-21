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
package com.dremio.service.flight;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/**
 * System Options for Dremio Flight Service.
 */
@Options
public interface DremioFlightServiceOptions {

  // Allows users enable/disable backpressure handling.
  TypeValidators.BooleanValidator ENABLE_BACKPRESSURE_HANDLING =
    new TypeValidators.BooleanValidator("flight.backpressure.handling.enable", true);

  // Allows users to configure UserSession lifetime. Default to 120 minutes.
  TypeValidators.PositiveLongValidator SESSION_EXPIRATION_TIME_MINUTES =
    new TypeValidators.PositiveLongValidator("flight.session.expiration.minutes", Integer.MAX_VALUE, 120);
}
