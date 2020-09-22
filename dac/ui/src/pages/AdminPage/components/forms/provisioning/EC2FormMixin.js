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
import {cloneDeep} from 'lodash/lang';
import {
  EC2_FORM_TAB_VLH, EC2_FIELDS_MAP
} from '@app/constants/provisioningPage/provisioningConstants';
import FormUtils from '@app/utils/FormUtils/FormUtils';

export const getInitValuesFromVlh = () => {
  const initValues = {};
  FormUtils.addInitValue(initValues, EC2_FIELDS_MAP.authMode, 'AUTO');
  FormUtils.addInitValue(initValues, EC2_FIELDS_MAP.useClusterPlacementGroup, true);
  FormUtils.addInitValue(initValues, EC2_FIELDS_MAP.instanceType, 'm5d.8xlarge');
  return initValues;
};

export default function(input) {

  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getVlh() {
      return cloneDeep(EC2_FORM_TAB_VLH);
    }
  });
}
