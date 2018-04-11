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
import { isEmptyValue } from 'utils/validation';
import transformRules from './transformRules';

class FieldsMappers {

  getCommonValues = (values, transform) => ({
    type: 'field',
    newColumnName: values.newFieldName,
    sourceColumnName: transform.get('columnName'),
    dropSourceColumn: values.dropSourceField
  });

  getRuleFromCards(cards, activeCard) {
    const card = cards[activeCard];
    return transformRules.getRuleMapper(card.type)(card);
  }

  getReplaceType = values => values.replaceType === 'NULL' ? 'NULL' : values.replaceSelectionType;

  getReplacementValue = values => {
    const replacementValue = !isEmptyValue(values.replacementValue) ? values.replacementValue : '';
    return values.replaceType === 'VALUE' ? replacementValue : null;
  }

  getSplitPosition = values => {
    const { index, maxFields } = values;
    const position = values.position ? values.position.toUpperCase() : null;
    if (values.position === 'All') {
      return { position, maxFields };
    } else if (values.position === 'Index') {
      return { position, index };
    }
    return { position };
  }

  getReplaceExact = (values, columnType) => ({
    replaceNull: values.replaceNull,
    replacementValue: this.getReplacementValue(values),
    replacedValuesList: values.replaceNull ? [] : values.replaceValues,
    replacementType: columnType,
    replaceType: this.getReplaceType(values)
  })

  setNullIfEmpty = (bound) => {
    if (bound === '') {
      return null;
    }
    return bound;
  }

  getReplaceRange = (values, columnType) => ({
    lowerBound: typeof values.lowerBound === 'object'
      ? values.lowerBound[0] : this.setNullIfEmpty(values.lowerBound),
    upperBound: typeof values.upperBound === 'object'
      ? values.upperBound[0] : this.setNullIfEmpty(values.upperBound),
    keepNull: values.keepNull,
    replacementType: columnType,
    replacementValue: this.getReplacementValue(values),
    lowerBoundInclusive: values.lowerBoundInclusive,
    upperBoundInclusive: values.upperBoundInclusive
  })

  getReplaceValues = (values, columnType) => ({
    replaceNull: values.hasOwnProperty('replaceNull') ? values.replaceNull : values.replaceType === 'NULL',
    replacementValue: this.getReplacementValue(values),
    replacedValuesList:  values.replaceValues,
    replacementType: columnType
  })

  getReplaceCustom = (values) => ({
    booleanExpression: values.booleanExpression,
    replacementValue: this.getReplacementValue(values)
  });
}

const fieldsMappers = new FieldsMappers();
export default fieldsMappers;
