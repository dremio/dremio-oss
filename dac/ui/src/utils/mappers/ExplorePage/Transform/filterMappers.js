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
import transformRules from './transformRules';

class FilterMappers {

  getCommonFilterValues = (item, transform) => ({
    type: 'filter',
    sourceColumnName: transform.get('columnName'),
    keepNull: item.keepNull,
    exclude: transform.get('transformType') === 'exclude'
  });

  getBound(value) {
    if (typeof value === 'object') {
      return value[0];
    }
    if (value === '') {
      return null;
    }

    return value;
  }

  mapFilterExcludeRange = (item, columnType) => ({
    type: 'Range',
    lowerBound: this.getBound(item.lowerBound),
    upperBound: this.getBound(item.upperBound),
    lowerBoundInclusive: item.lowerBoundInclusive,
    upperBoundInclusive: item.upperBoundInclusive,
    dataType: columnType
  });

  mapFilterExcludeCustom = item => ({
    type: 'Custom',
    expression: item.booleanExpression
  });

  mapFilterExcludeValues = (item, columnType) => ({
    type: 'Value',
    valuesList: item.replaceValues,
    dataType: columnType
  });

  mapFilterExcludePattern = item => ({
    type: 'Pattern',
    rule: transformRules.mapReplaceRule(item.cards[item.activeCard])
  });
}

const filterMappers = new FilterMappers();
export default filterMappers;
