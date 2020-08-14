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
import deepEqual from 'deep-equal';
import uuid from 'uuid';
import Immutable from 'immutable';
import { formatMessage } from 'utils/locale';
import { getUniqueName } from 'utils/pathUtils';
import { allMeasureTypes, cellTypesWithNoSum } from '@app/constants/AccelerationConstants';
import { ANY } from '@app/constants/DataTypes';

export const createReflectionFormValues = (opts, siblingNames = []) => {

  const reflection = {
    id: uuid.v4(), // need to supply a temp uuid so errors can be tracked
    tag: '',
    type: '',
    name: '',
    enabled: true,
    arrowCachingEnabled: false,
    distributionFields: [],
    partitionFields: [],
    sortFields: [],
    partitionDistributionStrategy: 'CONSOLIDATED',
    shouldDelete: false
  };

  if (opts.type === 'RAW') {
    reflection.displayFields = [];
  } else {
    reflection.dimensionFields = [];
    reflection.measureFields = [];
  }

  if (!opts.name) {
    reflection.name = siblingNames && getUniqueName(opts.type === 'RAW' ? la('Raw Reflection') : la('Aggregation Reflection'), proposedName => {
      return !siblingNames.includes(proposedName);
    });
  }

  // only copy values from opts if the key exists in our reduced reflection representation
  for (const key in reflection) {
    if (key in opts) {
      reflection[key] = opts[key];
    }
  }

  return reflection;
};

export const areReflectionFormValuesUnconfigured = reflectionFormValues => {
  const unconfiguredReflection = createReflectionFormValues({
    type: reflectionFormValues.type,

    // we consider the reflection id, tag, name, and arrowCachingEnabled inconsequential
    id: reflectionFormValues.id,
    tag: reflectionFormValues.tag,
    name: reflectionFormValues.name,
    enabled: reflectionFormValues.enabled
  });

  return deepEqual(reflectionFormValues, unconfiguredReflection);
};


export const areReflectionFormValuesBasic = (reflectionFormValues, dataset) => {
  reflectionFormValues = {...reflectionFormValues};

  const basicCopy = createReflectionFormValues({
    // these don't matter for basic v advanced mode:
    type: reflectionFormValues.type,
    id: reflectionFormValues.id,
    tag: reflectionFormValues.tag,
    name: reflectionFormValues.name,
    enabled: reflectionFormValues.enabled,
    arrowCachingEnabled: reflectionFormValues.arrowCachingEnabled
  });

  if (reflectionFormValues.type === 'RAW') {
    reflectionFormValues.displayFields.sort(fieldSorter);
    basicCopy.displayFields = dataset.fields.map(({name}) => ({name})).sort(fieldSorter);
  } else {
    reflectionFormValues = {
      ...reflectionFormValues,
      // these don't matter for basic v advanced mode:
      dimensionFields: basicCopy.dimensionFields, // ignoring granularity right now as even advanced does nothing with it (sync system is careful to preserve)
      measureFields: basicCopy.measureFields
    };
  }

  return deepEqual(reflectionFormValues, basicCopy);
};

export const fieldSorter = (a, b) => {
  if (a.name < b.name) {
    return -1;
  }
  if (a.name > b.name) {
    return 1;
  }
  return 0;
};

export const forceChangesForDatasetChange = (reflection, dataset) => {
  if (reflection.status.config !== 'INVALID') return {reflection};

  const lostFields = {};
  reflection = {...reflection};
  const validFields = new Set(dataset.fields.map(f => f.name));

  for (const feildList of 'sortFields partitionFields distributionFields displayFields dimensionFields measureFields'.split(' ')) {
    if (!reflection[feildList]) continue;
    reflection[feildList] = reflection[feildList].filter((field) => {
      if (validFields.has(field.name)) return true;
      lostFields[feildList] = lostFields[feildList] || [];
      lostFields[feildList].push(field);
      return false;
    });
  }

  // future: handle change field to invalid type (as-is; left to validation system to force a fix)

  return {reflection, lostFields};
};

export const findAllMeasureTypes = (fieldType) => {
  if (!fieldType) return null;

  const allTypes = Object.values(allMeasureTypes); //['MIN', 'MAX', 'SUM', 'COUNT', 'APPROX_COUNT_DISTINCT'];

  if (cellTypesWithNoSum.includes(fieldType)) {
    //filter creates a copy vs. splice, which would mutate allTypes
    return allTypes.filter(v => v !== allMeasureTypes.SUM);
  }

  return allTypes;
};

export const getDefaultMeasureTypes = (fieldType) => {
  const cellMeasureTypes = findAllMeasureTypes(fieldType);
  if (cellMeasureTypes.includes(allMeasureTypes.SUM)) {
    return [allMeasureTypes.SUM, allMeasureTypes.COUNT];
  }

  return [allMeasureTypes.COUNT];
};

// fixup any null or empty measureTypeLists
export const fixupReflection = (reflection, dataset) => {
  if (reflection.type === 'AGGREGATION' && reflection.measureFields && reflection.measureFields.length > 0) {
    for (const measureField of reflection.measureFields) {
      if (!measureField.measureTypeList) {
        measureField.measureTypeList = getDefaultMeasureTypes(getTypeForField(dataset, measureField.name));
      }
    }
  }
};

export const getTypeForField = (dataset, fieldName) => {
  if (!dataset.has('fields')) {
    return ANY;
  }

  return dataset.get('fields').find((elm) => {
    return elm.get('name') === fieldName;
  }).getIn(['type', 'name']);
};

const getTextWithFailureCount = (status, statusMessage) => {
  const msgId = (status.get('refresh') === 'MANUAL') ?
    'Reflection.StatusFailedNoReattempt' : 'Reflection.StatusFailedNonFinal';
  return formatMessage(msgId, {
    status: statusMessage,
    failCount: status.get('failureCount')
  });

};

export function getReflectionUiStatus(reflection) {
  if (!reflection) return;

  const status = reflection.get('status');

  let icon = 'WarningSolid';
  let text = '';
  let className = '';

  const statusMessage = status && status.get('availability') === 'AVAILABLE' ?
    formatMessage('Reflection.StatusCanAccelerate') : formatMessage('Reflection.StatusCannotAccelerate');

  if (!reflection.get('enabled')) {
    icon = 'Disabled';
    text = formatMessage('Reflection.StatusDisabled');
  } else if (status.get('config') === 'INVALID') {
    icon = 'ErrorSolid';
    text = formatMessage('Reflection.StatusInvalidConfiguration', {status: statusMessage});
  } else if (status.get('refresh') === 'GIVEN_UP') {
    icon = 'ErrorSolid';
    text = formatMessage('Reflection.StatusFailedFinal', {status: statusMessage});
  } else if (status.get('availability') === 'INCOMPLETE') {
    icon = 'ErrorSolid';
    text = formatMessage('Reflection.StatusIncomplete', {status: statusMessage});
  } else if (status.get('availability') === 'EXPIRED') {
    icon = 'ErrorSolid';
    text = formatMessage('Reflection.StatusExpired', {status: statusMessage});
  } else if (status.get('refresh') === 'RUNNING') {
    if (status.get('availability') === 'AVAILABLE') {
      icon = 'OKSolid';
      text = formatMessage('Reflection.StatusRefreshing', {status: statusMessage});
    } else {
      icon = 'Loader';
      text = formatMessage('Reflection.StatusBuilding', {status: statusMessage});
      className = 'spinner';
    }
  } else if (status.get('availability') === 'AVAILABLE') {
    if (status.get('failureCount') > 0) {
      icon = 'WarningSolid';
      text = getTextWithFailureCount(status, statusMessage);
    } else if (status.get('refresh') === 'MANUAL') {
      icon = 'OKSolid';
      text = formatMessage('Reflection.StatusManual', {status: statusMessage});
    } else {
      icon = 'OKSolid';
      text = formatMessage('Reflection.StatusCanAccelerate');
    }
  } else if (status.get('failureCount') > 0) {
    icon = 'WarningSolid';
    text = getTextWithFailureCount(status, statusMessage);
  } else if (status.get('refresh') === 'SCHEDULED') {
    icon = 'Ellipsis';
    text = formatMessage('Reflection.StatusBuilding', {status: statusMessage});
  } else if (status.get('refresh') === 'MANUAL') {
    icon = 'WarningSolid';
    text = formatMessage('Reflection.StatusManual', {status: statusMessage});
  }

  return Immutable.fromJS({
    icon,
    text,
    className
  });
}
