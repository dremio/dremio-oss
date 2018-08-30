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
import deepEqual from 'deep-equal';
import uuid from 'uuid';
import { getUniqueName } from 'utils/pathUtils';
import { allMeasureTypes, cellTypesWithNoSum } from 'constants/AccelerationConstants';
import { ANY } from 'constants/DataTypes';

export const createReflectionFormValues = (opts, siblingNames = []) => {

  const reflection = {
    id: uuid.v4(), // need to supply a temp uuid so errors can be tracked
    tag: '',
    type: '',
    name: '',
    enabled: true,
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

    // we consider the reflection id, tag, and name inconsequential
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
    enabled: reflectionFormValues.enabled
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
