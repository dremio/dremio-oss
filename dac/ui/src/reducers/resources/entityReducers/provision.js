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
import {
  LOAD_PROVISIONING_SUCCESS,
  LOAD_AWS_DEFAULTS_SUCCESS,
} from "#oss/actions/resources/provisioning";
import Immutable from "immutable";
import { decorateProvision } from "utils/decorators/resourceDecorators";
import { extraProvisionReducer } from "@inject/reducers/resources/entityReducers/provisionExtra";

export default function provisionReducer(state, action) {
  switch (action.type) {
    case LOAD_PROVISIONING_SUCCESS: {
      const provisions = action.payload.clusterList.reduce(
        (items, provision) => {
          return {
            ...items,
            [provision.id]: decorateProvision(Immutable.fromJS(provision)),
          };
        },
        {},
      );
      return state.mergeIn(["provision"], Immutable.Map(provisions));
    }

    case LOAD_AWS_DEFAULTS_SUCCESS: {
      return state.setIn(
        ["awsDefaults"],
        Immutable.fromJS({ awsProps: action.payload }),
      );
    }

    default:
      return extraProvisionReducer(state, action);
  }
}
