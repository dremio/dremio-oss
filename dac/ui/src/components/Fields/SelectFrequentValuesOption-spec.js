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
import SelectFrequentValuesOption from './SelectFrequentValuesOption';

describe('SelectFrequentValuesOption', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      checked: true,
      option: {
        percent: 25,
        value: 'foo'
      },
      maxPercent: 50,
      onCheck: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectFrequentValuesOption {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render Checkbox, Meter', () => {
    const wrapper = shallow(<SelectFrequentValuesOption {...commonProps}/>);
    const checkbox = wrapper.find('Checkbox');
    const meter = wrapper.find('Meter');
    expect(checkbox).to.have.length(1);
    expect(checkbox.prop('checked')).to.be.true;
    expect(meter).to.have.length(1);
    expect(meter.prop('value')).to.eql(commonProps.option.percent);
    expect(meter.prop('max')).to.eql(commonProps.maxPercent);
  });

  describe('#shouldComponentUpdate', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<SelectFrequentValuesOption {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should return true if field value for option change', () => {
      const nextProps = {
        checked: false,
        option: {
          value: 'bar'
        }
      };
      expect(instance.shouldComponentUpdate(nextProps)).to.be.true;
    });
    it('should return false if field value for option do not change', () => {
      const nextProps = {
        checked: true,
        option: {
          value: 'foo'
        }
      };
      expect(instance.shouldComponentUpdate(commonProps)).to.be.false;

      expect(instance.shouldComponentUpdate(nextProps)).to.be.false;
    });
  });

  describe('#renderLabelValue', () => {
    let testProps;
    beforeEach(() => {
      testProps = {
        ...minimalProps,
        checked: true,
        option: {
          percent: 50,
          value: undefined
        },
        maxPercent: 100
      };
    });

    it('should render proper color and text for undefined values', () => {
      const wrapper = shallow(<SelectFrequentValuesOption {...testProps}/>);
      const instance = wrapper.instance();
      expect(shallow(instance.renderLabelValue()).props().style.color).to.equal('#aaa');
      expect(shallow(instance.renderLabelValue()).props().children).to.equal('null');
    });
    it('should render proper color and text for empty text values', () => {
      const emptyProps = {
        ...testProps,
        option: {
          percent: 50,
          value: ''
        }
      };
      const wrapper = shallow(<SelectFrequentValuesOption {...emptyProps}/>);
      const instance = wrapper.instance();
      expect(shallow(instance.renderLabelValue()).props().style.color).to.equal('#aaa');
      expect(shallow(instance.renderLabelValue()).props().children).to.equal('empty text');
    });
    it('should render proper color and text for null values', () => {
      const normalProps = {
        ...testProps,
        option: {
          percent: 50,
          value: null
        }
      };
      const wrapper = shallow(<SelectFrequentValuesOption {...normalProps}/>);
      const instance = wrapper.instance();
      expect(shallow(instance.renderLabelValue()).props().style.color).to.equal('#aaa');
      expect(shallow(instance.renderLabelValue()).props().children).to.equal('null');
    });
    it('should render correctly for normal values', () => {
      const normalProps = {
        ...testProps,
        option: {
          percent: 50,
          value: 'foo'
        }
      };
      const wrapper = shallow(<SelectFrequentValuesOption {...normalProps}/>);
      const instance = wrapper.instance();
      expect(shallow(instance.renderLabelValue()).props().children).to.equal('foo');
    });
  });
});
