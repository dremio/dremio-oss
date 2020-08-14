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
import FormUtils from '@app/utils/FormUtils/FormUtils';

import {
  AWS_INSTANCE_TYPE_OPTIONS,
  ENGINE_SIZE_STANDARD_OPTIONS,
  CLUSTER_STATE, DREMIO_CUSTOM_REGION,
  EC2_AWS_CONNECTION_PROPS,
  EC2_AWS_PROPS,
  EC2_DYNAMIC_CONFIG_FIELDS,
  EC2_AWS_PROPLIST_FIELDS,
  EC2_UI_FIELDS,
  EC2_FIELDS_MAP
} from '@app/constants/provisioningPage/provisioningConstants';

import { EC2_CLUSTER_FIELDS } from 'dyn-load/constants/provisioningPage/provisioningConstants';


export function isEditMode(provision) {
  return !!(provision && provision.size);
}

export function isRestartRequired(provision, isDirty) {
  const editMode = isEditMode(provision);
  const isRunning = provision.get('currentState') === CLUSTER_STATE.running;
  return editMode && isRunning && isDirty;
}

export function addPropsForSave(accumulator, fieldNames, values) {
  return fieldNames.reduce((accum, field) => {
    accum[field] = values[field];
    return accum;
  }, accumulator);
}

export function getInstanceTypeValue(label) {
  const option = AWS_INSTANCE_TYPE_OPTIONS.find(o => o.label === label);
  return option && option.value || label;
}

export function engineSizeValue(containerCount) {
  const matchingOption = ENGINE_SIZE_STANDARD_OPTIONS.find(option => option.value === containerCount);
  return matchingOption ? containerCount : -1;
}

export function getInitPropListValue(provision, prop) {
  const listProp = provision.getIn(['awsProps', prop]);
  if (!listProp) return [];

  return listProp.toJS().map((listEntry, idx) => {
    return {id: idx, name: listEntry.key, value: listEntry.value};
  });
}

export function getInitValuesFromProvision(provision, initValues = {}) {
  EC2_CLUSTER_FIELDS.forEach(prop => {
    FormUtils.addInitValue(initValues, prop, provision.get(prop));
  });
  EC2_DYNAMIC_CONFIG_FIELDS.forEach(prop => {
    FormUtils.addInitValue(initValues, prop, provision.getIn(['dynamicConfig', prop]));
  });
  //engineSize selector does not have direct provision property, it is mapped to containerCount
  FormUtils.addInitValue(initValues, EC2_FIELDS_MAP.engineSize, engineSizeValue(provision.getIn(['dynamicConfig', 'containerCount'])));

  EC2_AWS_PROPS.forEach(prop => {
    FormUtils.addInitValue(initValues, prop, provision.getIn(['awsProps', prop]));
  });
  EC2_AWS_CONNECTION_PROPS.forEach(prop => {
    FormUtils.addInitValue(initValues, prop, provision.getIn(['awsProps', 'connectionProps', prop]));
  });
  EC2_AWS_PROPLIST_FIELDS.forEach(prop => {
    FormUtils.addInitValue(initValues, prop, getInitPropListValue(provision, prop));
  });

  return initValues;
}

const prepareDynamicConfigForSave = (values) => {
  const size = values[EC2_UI_FIELDS[0]]; //engineSize
  if (size === -1) {
    // with custom size option the regular containerCount value is used
    return addPropsForSave({}, EC2_DYNAMIC_CONFIG_FIELDS, values);
  } else {
    // otherwise containerCount is a value from engineSize selector
    return size ? {containerCount: size} : {};
  }
};

const prepareInstanceTypeForSave = (values) => {
  const size = values[EC2_UI_FIELDS[0]]; //engineSize
  if (size === -1) {
    return {}; // instanceType property is visible and used explicitly
  } else {
    return {instanceType: AWS_INSTANCE_TYPE_OPTIONS[0].value}; // using default 'Standard'
  }
};

export const preparePropertyListFieldForSave = (values) => {
  const awsTags = values.awsTags
    ? values.awsTags.map(tag => ({key: tag.name, value: tag.value}))
    : [];
  return { awsTags };
};

export function prepareProvisionValuesForSave(values) {
  const payload = {
    clusterType: 'EC2',
    ...addPropsForSave({}, EC2_CLUSTER_FIELDS, values),
    dynamicConfig: {
      ...prepareDynamicConfigForSave(values)
    },
    yarnProps: null,
    awsProps: {
      ...addPropsForSave({}, EC2_AWS_PROPS, values),
      ...prepareInstanceTypeForSave(values),
      ...preparePropertyListFieldForSave(values),
      connectionProps: {
        ...addPropsForSave({}, EC2_AWS_CONNECTION_PROPS, values)
      }
    }
  };
  //if region is not endpoint-override, remove endpoint from the payload
  if (payload.awsProps.connectionProps.region !== DREMIO_CUSTOM_REGION
    && Object.keys(payload.awsProps.connectionProps).includes('endpoint')) {
    delete payload.awsProps.connectionProps.endpoint;
  }
  //if authType is auto, remove key-secret from the payload
  if (payload.awsProps.connectionProps.authMode === 'AUTO') {
    if (payload.awsProps.connectionProps.accessKey) {
      delete payload.awsProps.connectionProps.accessKey;
    }
    if (payload.awsProps.connectionProps.secretKey) {
      delete payload.awsProps.connectionProps.secretKey;
    }
  }
  return payload;
}
