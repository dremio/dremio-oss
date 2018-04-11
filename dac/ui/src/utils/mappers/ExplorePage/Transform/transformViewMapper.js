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
import datasetUtils from 'utils/resourcePathUtils/dataset';
import { isEmptyValue } from 'utils/validation';

// todo: loc

class TransformViewMapper {
  mapTransformRules(data, type) {
    const hash = {
      extract: () => this.transformExportMapper(data),
      'extract_map': () => this.transformExtractMapMapper(data),
      'extract_list': () => this.transformExtractListMapper(data),
      replace: () => this.transformReplaceMapper(data),
      keeponly: () => this.transformReplaceMapper(data, 'keeponly'),
      exclude: () => this.transformReplaceMapper(data, 'exclude'),
      split: () => this.transformSplitMapper(data)
    };
    if (hash[type]) {
      return hash[type]();
    }
  }

  transformExportMapper(data) {
    const directionHash = {
      FROM_THE_START: 'Start',
      FROM_THE_END: 'End'
    };
    return data.cards.map(item => {
      return {
        type: item.rule.type,
        description: item.description,
        matchedCount: item.matchedCount,
        unmatchedCount: item.unmatchedCount,
        examplesList: (item.examplesList || []).map(example => {
          return {
            description: item.description,
            text: example.text,
            positionList: example.positionList
          };
        }),
        position: {
          startIndex: {
            value: item.rule.position && !isEmptyValue(item.rule.position.startIndex.value)
              ? item.rule.position.startIndex.value : '',
            direction: directionHash[item.rule.position && item.rule.position.startIndex.direction] || ''
          },
          endIndex: {
            value: item.rule.position && !isEmptyValue(item.rule.position.endIndex.value)
              ? item.rule.position.endIndex.value : '',
            direction: directionHash[item.rule.position && item.rule.position.endIndex.direction] || ''
          }
        },
        pattern: {
          pattern: item.rule.pattern && item.rule.pattern.pattern,
          value: item.rule.pattern && this.getValueForPattern(item.rule.pattern.index, item.rule.pattern.indexType)
        }
      };
    });
  }

  // used in transformExportMapper
  getValueForPattern(index, indexType) {
    if (indexType === 'CAPTURE_GROUP') {
      return {
        label: 'Capture Group…',
        value: 'CAPTURE_GROUP',
        index
      };
    } else if (index === 0 && indexType === 'INDEX') {
      return {
        label: 'First',
        value: 'FIRST',
        index
      };
    } else if (index === 0 && indexType === 'INDEX_BACKWARDS') {
      return {
        label: 'Last',
        value: 'LAST',
        index
      };
    }

    return {
      label: 'Index…',
      value: 'INDEX',
      index
    };
  }

  transformExtractMapMapper(data) {
    return data.cards.map((item) => {
      return {
        path: item.rule.path,
        description: item.description,
        matchedCount: item.matchedCount,
        unmatchedCount: item.unmatchedCount,
        examplesList: (item.examplesList || []).map(example => {
          return {
            description: item.description,
            text: example.text,
            positionList: example.positionList
          };
        }),
        type: 'map'
      };
    });
  }

  transformExtractListMapper(data) {
    const directionHash = {
      FROM_THE_START: 'Start',
      FROM_THE_END: 'End'
    };
    return data.cards.map(item => {
      return {
        type: item.rule.type,
        description: item.description,
        matchedCount: item.matchedCount,
        unmatchedCount: item.unmatchedCount,
        examplesList: (item.examplesList || []).map(example => {
          return {
            description: item.description,
            text: example.text,
            positionList: example.positionList
          };
        }),
        multiple: {
          startIndex: {
            value: item.rule.selection && !isEmptyValue(item.rule.selection.start.value)
              ? item.rule.selection.start.value : '',
            direction: directionHash[item.rule.selection && item.rule.selection.start.direction] || ''
          },
          endIndex: {
            value: item.rule.selection && !isEmptyValue(item.rule.selection.end.value)
              ? item.rule.selection.end.value : '',
            direction: directionHash[item.rule.selection && item.rule.selection.end.direction] || ''
          }
        },
        single: {
          startIndex: {
            value: item.rule.index,
            direction: 'Start'
          }
        }
      };
    });
  }

  transformReplaceMapper(data, type) {
    const cards = data.cards.map(item => {
      return {
        type: type || 'replace',
        description: item.description,
        matchedCount: item.matchedCount,
        unmatchedCount: item.unmatchedCount,
        examplesList: (item.examplesList || []).map(example => {
          return {
            text: example.text,
            positionList: example.positionList
          };
        }),
        replace: {
          ignoreCase: item.rule.ignoreCase,
          selectionPattern: item.rule.selectionPattern,
          selectionType: item.rule.selectionType
        }
      };
    });
    const values = data.values && data.values.availableValues && data.values.availableValues.map( item => {
      return item;
    }) || [];
    const unmatchedCount = data.values && data.values.availableValues && data.values.unmatchedValues;
    const matchedCount = data.values && data.values.availableValues && data.values.matchedValues;
    return { cards, values: { values, unmatchedCount, matchedCount } };
  }

  transformSplitMapper(data) {
    return data.cards.map(item => {
      return {
        type: 'split',
        matchedCount: item.matchedCount,
        unmatchedCount: item.unmatchedCount,
        description: item.description,
        rule: {
          pattern: item.rule.pattern,
          matchType: item.rule.matchType,
          ignoreCase: item.rule.ignoreCase
        },
        examplesList: (item.examplesList || []).map(example => {
          return {
            description: item.description,
            text: example.text,
            positionList: example.positionList
          };
        })
      };
    });
  }

  mapRecJoin(payload) {
    return (payload.recommendations || []).map(data => {
      const dpath = datasetUtils.toFullPath(data.dataset.resourcePath);
      return {
        joinType: data.joinType,
        matchingKey: data.matchingKeys,
        dataset: {
          dpath,
          datasetName: data.dataset.datasetName,
          resourcePath: data.dataset.resourcePath,
          version: data.dataset.datasetConfig.version
        }
      };
    });
  }
}

const transformViewMapper = new TransformViewMapper();
export default transformViewMapper;
