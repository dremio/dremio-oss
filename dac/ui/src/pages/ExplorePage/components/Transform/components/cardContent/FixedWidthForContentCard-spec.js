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
import { mount } from 'enzyme';

import FixedWidthForContentCard from './FixedWidthForContentCard';

describe('testing FixedWidthForContentCard', () => {
  let examples;
  const WIDTH_CARDS = 430;
  beforeEach(() => {
    examples = [
      [
        {
          offset: 36,
          length: 5,
          text: '{"Tuesday":{"close":"19:00","open":"10:00"},"Friday":{"close":"19:00","open":"10:00"},"Monday":{}}'
        },
        [
          ['ay":{"close":"19:00","open":"'],
          ['10:00'],
          ['"},"Friday":{"close":"19:00",']
        ]
      ],
      [
        {
          offset: 36,
          length: 5,
          text: '{"Tuesday":{"close":"19:00","open":"10:00"},"Friday":{},"Monday":{"close":"02:00","open":"08:00"}}'
        },
        [
          ['ay":{"close":"19:00","open":"'],
          ['10:00'],
          ['"},"Friday":{},"Monday":{"clo']
        ]
      ],
      [
        {
          offset: 36,
          length: 5,
          text: '{"Tuesday":{"close":"19:00","open":"10:00"},"Friday":{},"Monday":{"close":"14:30","open":"06:00"}}'
        },
        [
          ['ay":{"close":"19:00","open":"'],
          ['10:00'],
          ['"},"Friday":{},"Monday":{"clo']
        ]
      ],
      [
        {
          offset: 10,
          length: 22,
          text: '"["Bars","American (Traditional)","Nightlife","Lounges","Restaurants"]"'
        },
        [
          ['"["Bars","'],
          ['American (Traditional)'.replace(/ /g, '\u00a0')],
          ['","Nightlife","Lounges","Restau']
        ]
      ]
    ];
  });

  it('should return array with selected text and with text around', () => {
    for (const example of examples) {
      const result = FixedWidthForContentCard.getExampleTextParts(example[0], WIDTH_CARDS);
      expect(result).to.eql(example[1]);
    }
  });

  it('calls componentDidMount', () => {
    sinon.spy(FixedWidthForContentCard.prototype, 'componentDidMount');
    mount(<FixedWidthForContentCard example={examples[0][0]} index={0}/>);
    expect(FixedWidthForContentCard.prototype.componentDidMount.calledOnce).to.equal(true);
  });

  it('should render string for card', () => {
    examples.forEach((example, index) => {
      const wrapper = mount(<FixedWidthForContentCard example={example[0]} index={index}/>);
      expect(wrapper.find('.fixed_width').children()).to.have.length(3);

      // width 0
      expect(wrapper.find('.fixed_width').children().at(0).text()).to.equal('');
      expect(wrapper.find('.fixed_width').children().at(1).text()).to.equal(example[1][1][0]);
      expect(wrapper.find('.fixed_width').children().at(2).text()).to.equal('');

      wrapper.setState({width: 1});
      expect(wrapper.find('.fixed_width').children().at(0).text()).to.equal('');
      expect(wrapper.find('.fixed_width').children().at(1).text()).to.equal(example[1][1][0]);
      expect(wrapper.find('.fixed_width').children().at(2).text()).to.equal('');

      wrapper.setState({width: WIDTH_CARDS});
      expect(wrapper.find('.fixed_width').children().at(0).text()).to.equal(example[1][0][0]);
      expect(wrapper.find('.fixed_width').children().at(1).text()).to.equal(example[1][1][0]);
      expect(wrapper.find('.fixed_width').children().at(2).text()).to.equal(example[1][2][0]);
    });
  });
});
