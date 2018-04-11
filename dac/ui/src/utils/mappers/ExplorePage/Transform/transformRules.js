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
class TransformRules {
  mapPositionRule(rule) {
    const hash = {
      Start: 'FROM_THE_START',
      End: 'FROM_THE_END'
    };
    return {
      type: 'position',
      position: {
        startIndex: {
          value: rule.position.startIndex.value || 0,
          direction: hash[rule.position.startIndex.direction] || 'FROM_THE_START'
        },
        endIndex: {
          value: rule.position.endIndex.value || 0,
          direction: hash[rule.position.endIndex.direction] || 'FROM_THE_START'
        }
      }
    };
  }

  mapPatternRule(rule) {
    const hash = {
      INDEX: 'INDEX',
      FIRST: 'INDEX',
      LAST: 'INDEX_BACKWARDS',
      CAPTURE_GROUP: 'CAPTURE_GROUP'
    };
    return {
      type: 'pattern',
      pattern: {
        ignoreCase: rule.ignoreCase,
        pattern: rule.pattern.pattern || '',
        index: rule.pattern.value && rule.pattern.value.index || 0,
        indexType: rule.pattern.value && hash[rule.pattern.value.value] || 'INDEX'
      }
    };
  }

  mapReplaceRule = rule => ({
    selectionPattern: rule.replace.selectionPattern,
    selectionType: rule.replace.selectionType,
    ignoreCase: rule.replace.ignoreCase || false
  });

  mapSplitRule = item => ({
    ignoreCase: item.rule.ignoreCase,
    matchType: item.rule.matchType,
    pattern: item.rule.pattern
  });

  mapSingleRule = rule => ({
    type: 'single',
    index: rule.single.startIndex.value
  });

  mapExtractMapRule = rule => ({path: rule.path});

  mapMultipleRule(rule) {
    const hash = {
      Start: 'FROM_THE_START',
      End: 'FROM_THE_END'
    };
    return {
      type: 'multiple',
      selection: {
        start: {
          value: rule.multiple.startIndex.value || 0,
          direction: hash[rule.multiple.startIndex.direction] || 'FROM_THE_START'
        },
        end: {
          value: rule.multiple.endIndex.value || 0,
          direction: hash[rule.multiple.endIndex.direction] || 'FROM_THE_START'
        }
      }
    };
  }

  getRuleMapper = ruleType => ({
    position: this.mapPositionRule,
    pattern: this.mapPatternRule,
    multiple: this.mapMultipleRule,
    replace: this.mapReplaceRule,
    split: this.mapSplitRule,
    single: this.mapSingleRule,
    keeponly: this.mapReplaceRule,
    exclude: this.mapReplaceRule,
    map: this.mapExtractMapRule
  }[ruleType]);

}

const transformRules = new TransformRules();
export default transformRules;
