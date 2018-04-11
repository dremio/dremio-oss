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
import extractResponse from 'mockResponses/transform/extract.json';
import replaceResponse from 'mockResponses/transform/replace.json';
import excludeResponse from 'mockResponses/transform/exclude.json';
import splitResponse from 'mockResponses/transform/split.json';
import keeponlyResponse from 'mockResponses/transform/keeponly.json';

import transformViewMapper from './transformViewMapper';


describe('transformViewMapper', () => {
  describe('transformExtractListMapper', () => {
    let expectedResult;
    beforeEach(() => {
      expectedResult = [{
        type: 'position',
        description: 'Elements: 0 - 4',
        matchedCount: 100,
        unmatchedCount: 0,
        examplesList: [
          {
            description: 'Elements: 0 - 4',
            text: 'address0',
            positionList: [{offset: 0, length: 5}]
          },
          {
            description: 'Elements: 0 - 4',
            text: 'address1',
            positionList: [{offset: 0, length: 5}]
          },
          {
            description: 'Elements: 0 - 4',
            text: 'address2',
            positionList: [{offset: 0, length: 5}]
          }
        ],
        position: {
          startIndex: {
            value: 0,
            direction: 'Start'
          },
          endIndex: {
            value: 4,
            direction: 'Start'
          }
        },
        pattern: {
          pattern: undefined,
          value: undefined
        }
      }];
    });
    it('should map extract response for cards', () => {
      const mappedResult = transformViewMapper.transformExportMapper(extractResponse);
      expect([mappedResult[0]]).to.eql(expectedResult);
    });
  });
  describe('transformReplaceMapper', () => {
    let card, value, unmatchedCount, matchedCount;
    beforeEach(() => {
      card = {
        type: 'replace',
        description: 'Contains addr',
        matchedCount: 100,
        unmatchedCount: 0,
        examplesList: [
          {
            text: 'address0',
            positionList: [{offset:0, length:4}]
          },
          {
            text: 'address1',
            positionList: [{offset:0, length: 4}]
          },
          {
            text: 'address2',
            positionList: [{offset: 0, length: 4}]
          }
        ],
        replace: {
          selectionType: 'CONTAINS',
          selectionPattern: 'addr',
          ignoreCase: false
        }
      };
      value = {type: 'TEXT', value: 'address0', percent: 1, count: 1};
      matchedCount = 0;
      unmatchedCount = 100;

    });
    it('should map replace response for cards', () => {
      const mappedResult = transformViewMapper.transformReplaceMapper(replaceResponse);
      expect(mappedResult.cards[0]).to.eql(card);
      expect(mappedResult.values.values[0]).to.eql(value);
      expect(mappedResult.values.matchedCount).to.eql(matchedCount);
      expect(mappedResult.values.unmatchedCount).to.eql(unmatchedCount);
    });
    it('should map keeponly response for cards', () => {
      const keeponlyCard = {
        ...card,
        type: 'keeponly'
      };
      const mappedResult = transformViewMapper.transformReplaceMapper(keeponlyResponse, 'keeponly');
      expect(mappedResult.cards[0]).to.eql(keeponlyCard);
    });
    it('should map exclude response for cards', () => {
      const excludeCard = {
        ...card,
        type: 'exclude'
      };
      const mappedResult = transformViewMapper.transformReplaceMapper(excludeResponse, 'exclude');
      expect(mappedResult.cards[0]).to.eql(excludeCard);
    });
  });
  describe('transformSplitMapper', () => {
    let card;
    beforeEach(() => {
      card = {
        type: 'split',
        matchedCount: 100,
        unmatchedCount: 0,
        description: 'Exactly matches "addre"',
        rule: {
          pattern: 'addre',
          matchType: 'exact',
          ignoreCase: false
        },
        examplesList:[
          {
            description: 'Exactly matches \"addre\"',
            text: 'address0',
            positionList: [{offset:0, length:5}]
          },
          {
            description: 'Exactly matches "addre"',
            text: 'address1',
            positionList: [{offset: 0, length: 5}]
          },
          {
            description: 'Exactly matches "addre"',
            text: 'address2',
            positionList: [{offset: 0, length: 5}]
          }
        ]
      };
    });
    it('should map split response for cards', () => {
      const mappedResult = transformViewMapper.transformSplitMapper(splitResponse);
      expect(mappedResult[0]).to.eql(card);
    });
  });
});
