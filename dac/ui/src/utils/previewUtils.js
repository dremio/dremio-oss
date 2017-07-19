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
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';
import { LIST, MAP } from 'constants/DataTypes';

export const METHODS = {
  PATTERN: 'Pattern',
  VALUES: 'Values',
  CUSTOM: 'Custom Condition',
  EXACT: 'Exact',
  RANGE: 'Range'
};

export const TRANSFORM_TYPES = {
  EXTRACT: 'extract',
  REPLACE: 'replace',
  SPLIT: 'split',
  KEEPONLY: 'keeponly',
  EXCLUDE: 'exclude'
};

export const ReplaceInitialValues = {
  [METHODS.PATTERN]: (columnName, cards) => ({
    newFieldName: columnName,
    dropSourceField: true,
    activeCard: 0,
    cards: cards.toJS(),
    replaceValue: '',
    replaceSelectionType: 'VALUE',
    replaceType: 'VALUE'
  }),
  [METHODS.VALUES]: (columnName) => ({
    newFieldName: columnName,
    dropSourceField: true,
    replaceValues: {},
    replaceType: 'VALUE',
    replaceSelectionType: 'VALUE'
  }),
  [METHODS.CUSTOM]: (columnName) => ({
    newFieldName: columnName,
    dropSourceField: true,
    replaceValue: '',
    replaceType: 'VALUE',
    replaceSelectionType: 'VALUE',
    booleanExpression: 'value IS NOT null \nOR\nvalue IS null'
  }),
  [METHODS.EXACT]: (columnName, cards, selection) => {
    //TODO Check more ReplaceExactForm component
    const cellText = selection.get('cellText');
    const defaultValue = cards && cards.get && cards.get('values') &&
      cards.get('values').get(0) && cards.get('values').get(0).get('value');
    return {
      newFieldName: columnName,
      dropSourceField: true,
      replaceValues: [cellText || defaultValue || 0],
      replaceValue: 0,
      replaceNull: false,
      replaceType: 'VALUE'
    };
  },
  [METHODS.RANGE]: (columnName) => ({
    newFieldName: columnName,
    dropSourceField: true,
    upperBound: 0,
    lowerBound: 0,
    keepNull: false,
    replaceValue: 0,
    replaceType: 'VALUE',
    replaceSelectionType: 'VALUE'
  })
};

export function getTransformInitialValues(transform, cards) {
  const transformType = transform.get('transformType');
  const columnName = transform.get('columnName');
  switch (transformType) {
  case TRANSFORM_TYPES.SPLIT:
    return {
      cards: cards.toJS().map((card) => {
        const { type, rule } = card;
        return { type, rule };
      }),
      newFieldName: columnName,
      dropSourceField: true,
      activeCard: 0,
      position: 'First',
      index: 0,
      maxFields: 10
    };
  case TRANSFORM_TYPES.EXTRACT:
    return {
      newFieldName: columnName,
      dropSourceField: true,
      activeCard: 0,
      cards: cards.toJS()
    };
  case TRANSFORM_TYPES.EXCLUDE:
  case TRANSFORM_TYPES.KEEPONLY:
  case TRANSFORM_TYPES.REPLACE:
    return getReplaceInitialValues(transform, cards);
  default:
    return null;
  }
}

export function getReplaceInitialValues(transform, cards) {
  const method = transform.get('method');
  const columnName = transform.get('columnName');
  const selection = transform.get('selection');

  return ReplaceInitialValues[method](columnName, cards, selection);
}

export function getEditTextInitialValues(detailType, columnName) {
  return {
    newFieldName: columnName,
    action: detailType !== 'TRIM_WHITE_SPACES' ? 'UPPERCASE' : 'BOTH',
    dropSourceField: true
  };
}

export const ReplaceMapValues = {
  [METHODS.PATTERN]: (values, transform) => {
    const transformType = transform.get('transformType');
    return transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, transform),
        fieldTransformation: {
          type: 'ReplacePattern',
          replaceType: fieldsMappers.getReplaceType(values),
          replacementValue: fieldsMappers.getReplacementValue(values),
          rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludePattern(values, transform)
      };
  },
  [METHODS.VALUES]: (values, transform) => {
    const transformType = transform.get('transformType');
    let { replaceValues } = values;
    replaceValues =  Object.keys(replaceValues).filter((value) => values[value]);
    const resultValues = {
      ...values,
      replaceValues
    };

    return transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(resultValues, transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceValues(resultValues, transform.get('columnType'))
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(resultValues, transform),
        filter: filterMappers.mapFilterExcludeValues(resultValues, transform.get('columnType'))
      };
  },
  [METHODS.CUSTOM]: (values, transform) => {
    const transformType = transform.get('transformType');
    return transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, transform),
        fieldTransformation: {
          type: 'ReplaceCustom',
          ...fieldsMappers.getReplaceCustom(values)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeCustom(values, transform)
      };
  },
  [METHODS.EXACT]: (values, transform) => {
    const transformType = transform.get('transformType');
    const columnType = transform.get('columnType');
    return transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceExact(values, columnType)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeValues(values, columnType)
      };
  },
  [METHODS.RANGE]: (values, transform) => {
    const transformType = transform.get('transformType');
    return transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, transform),
        fieldTransformation: {
          type: 'ReplaceRange',
          ...fieldsMappers.getReplaceRange(values, transform.get('columnType'))
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeRange(values, transform.get('columnType'))
      };
  }
};

export function mapValues(values, transform) {
  const transformType = transform.get('transformType');
  switch (transformType) {
  case TRANSFORM_TYPES.SPLIT:
    return {
      ...fieldsMappers.getCommonValues(values, transform),
      fieldTransformation: {
        type: 'Split',
        rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard),
        ...fieldsMappers.getSplitPosition(values)
      }
    };
  case TRANSFORM_TYPES.EXTRACT: {
    const columnType = transform.get('columnType');
    let extractType = '';
    if (columnType === LIST) {
      extractType = 'ExtractList';
    } else if (columnType === MAP) {
      extractType = 'ExtractMap';
    } else {
      extractType = 'extract';
    }
    return {
      ...fieldsMappers.getCommonValues(values, transform),
      fieldTransformation: {
        type: extractType,
        rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
      }
    };
  }
  case TRANSFORM_TYPES.EXCLUDE:
  case TRANSFORM_TYPES.KEEPONLY:
  case TRANSFORM_TYPES.REPLACE:
    return mapReplaceValues(values, transform);
  default:
    return null;
  }
}

function mapReplaceValues(values, transform) {
  const method = transform.get('method');

  return ReplaceMapValues[method](values, transform);
}
