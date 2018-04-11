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
import Immutable from 'immutable';

import CardContentView from './CardContentView';

describe('CardContentView', () => {

  describe('getExampleTextParts', () => {
    it('should split out the highlighted part', () => {
      const examples = [
        [
          {
            positionList: [{ offset: 7, length: 1 }],
            text: 'address1'
          },
          [
            { text: 'address' },
            { text: '1', highlight: true }
          ]
        ],
        [
          {
            positionList: [{ offset: 0, length: 7 }],
            text: 'address1'
          },
          [
            { text: 'address', highlight: true },
            { text: '1' }
          ]
        ],
        [
          {
            positionList: [{ offset: 2, length: 4 }],
            text: 'address1'
          },
          [
            { text: 'ad' },
            { text: 'dres', highlight: true },
            { text: 's1' }
          ]
        ],
        [
          {
            positionList: [{ offset: 0, length: 20 }],
            text: 'address1'
          },
          [
            { text: 'address1', highlight: true }
          ]
        ],
        [
          {
            positionList: [{ offset: 1, length: 1 }, { offset: 2, length: 1 }],
            text: 'address1'
          },
          [
            { text: 'a' },
            { text: 'd', highlight: true },
            { text: 'd', highlight: true },
            { text: 'ress1' }
          ]
        ]
      ];
      for (const example of examples) {
        const exampleMap = Immutable.fromJS(example[0]);
        expect(CardContentView.getExampleTextParts(exampleMap)).to.eql(example[1]);
      }
    });
  });
});
