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
import { shallow } from 'enzyme';

import TransformRange from './TransformRange';

describe('TransformRange', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      columnType: 'FLOAT',
      fields: {
        lowerBound: {
          value: 0,
          onChange: sinon.spy()
        },
        upperBound: {
          value: 10,
          onChange: sinon.spy()
        },
        keepNull: {
          value: false
        }
      },
      data: [
        {x: 0, range: { lowerLimit: 0, upperLimit: 0 }},
        {x: 1, range: { lowerLimit: 1, upperLimit: 1 }},
        {x: 2, range: { lowerLimit: 2, upperLimit: 2 }},
        {x: 3, range: { lowerLimit: 3, upperLimit: 3 }},
        {x: 4, range: { lowerLimit: 4, upperLimit: 4 }},
        {x: 5, range: { lowerLimit: 5, upperLimit: 5 }},
        {x: 6, range: { lowerLimit: 6, upperLimit: 6 }},
        {x: 7, range: { lowerLimit: 7, upperLimit: 7 }},
        {x: 8, range: { lowerLimit: 8, upperLimit: 8 }},
        {x: 9, range: { lowerLimit: 9, upperLimit: 9 }},
        {x: 10, range: { lowerLimit: 10, upperLimit: 10 }}
      ]
    };
  });

  it('renders', () => {
    const wrapper = shallow(<TransformRange {...commonProps}/>);
    expect(wrapper.type()).to.eql('div');
  });

  describe('componentWillReceiveProps', () => {
    it('should set state with new domain if data is changed', () => {
      const wrapper = shallow(<TransformRange {...commonProps}/>);
      const instance = wrapper.instance();
      const props = {
        data: [
          {x: 0, range: { lowerLimit: 0, upperLimit: 0 }},
          {x: 1, range: { lowerLimit: 1, upperLimit: 1 }},
          {x: 2, range: { lowerLimit: 2, upperLimit: 2 }},
          {x: 3, range: { lowerLimit: 3, upperLimit: 3 }}
        ]
      };
      const prevDomain = instance.domain;
      instance.componentWillReceiveProps(props);
      const curDomain = instance.domain;
      expect(prevDomain !== curDomain).to.be.true;
    });
  });

  describe('getChartOffsetForBound', () => {
    it('return null when no data', () => {
      expect(TransformRange.getChartOffsetForBound({
        bound: 5,
        width: 1000,
        data: []
      })).to.equal(null);
    });

    it('returns correct values', () => {
      const data = commonProps.data.map((item) => item.x);
      expect(Math.round(TransformRange.getChartOffsetForBound({
        bound: 5,
        width: 1000,
        data
      }))).to.equal(500);
      expect(TransformRange.getChartOffsetForBound({
        bound: 11,
        width: 1000,
        data
      })).to.equal(1000);
    });

    it('handles string values', () => {
      const stringData = commonProps.data.map(item => String(item.x));
      expect(Math.round(TransformRange.getChartOffsetForBound({
        bound: 5,
        width: 1000,
        data: stringData
      }))).to.equal(500);
      expect(TransformRange.getChartOffsetForBound({
        bound: 11,
        width: 1000,
        data: stringData
      })).to.equal(1000);
    });
  });

  describe('validateBound', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<TransformRange {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should return true if lowerBound less then newValue for right bound', () => {
      expect(instance.validateBound(true)(10)).to.be.true;
    });
    it('should return false if upperBound less then newValue for left bound', () => {
      expect(instance.validateBound(false)(20)).to.be.false;
    });
    it('should return true if lowerBound less then newValue for right bound, string value', () => {
      expect(instance.validateBound(true)('10')).to.be.true;
    });
    it('should return false if upperBound less then newValue for left bound, string value', () => {
      expect(instance.validateBound(false)('20')).to.be.false;
    });
    it('should return false if newValue is string with letters', () => {
      expect(instance.validateBound(false)('abc')).to.be.false;
    });
    it('should return false if lowerBound greater then newValue for right bound', () => {
      expect(instance.validateBound(true)(-10)).to.be.false;
    });
    it('should return true if upperBound greater then newValue for left bound', () => {
      expect(instance.validateBound(false)(-10)).to.be.true;
    });
    it('should return true if upperBound greater then newValue for left bound', () => {
      expect(instance.validateBound(false)(-10)).to.be.true;
    });
    it('should return true if upperBound is empty for left bound', () => {
      wrapper.setProps({
        fields: {
          ...commonProps.fields,
          upperBound: {
            value: undefined
          }
        }
      });
      expect(instance.validateBound(false)(-10)).to.be.true;
    });
    it('should return true if lowerBound is empty for right bound', () => {
      wrapper.setProps({
        fields: {
          ...commonProps.fields,
          lowerBound: {
            value: undefined
          }
        }
      });
      expect(instance.validateBound(true)(-10)).to.be.true;
    });
  });
  describe('renderKeepNullCheckbox', () => {
    it('should return nothing if transform type is replace', () => {
      const replaceProps = {...commonProps, isReplace: true};
      const wrapper = shallow(<TransformRange {...replaceProps}/>);
      const instance = wrapper.instance();
      expect(instance.renderKeepNullCheckbox()).to.be.undefined;
    });
    it('should return the keepnull checkbox and line if transform type is not replace', () => {
      const replaceProps = {...commonProps, isReplace: false};
      const wrapper = shallow(<TransformRange {...replaceProps}/>);
      const instance = wrapper.instance();
      expect(instance.renderKeepNullCheckbox()).to.not.be.undefined;
    });
  });
});
