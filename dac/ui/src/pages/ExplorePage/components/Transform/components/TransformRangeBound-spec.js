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
import { INTEGER } from 'constants/DataTypes';
import TransformRangeBound from './TransformRangeBound';

describe('TransformRangeBound', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      defaultValue: '0',
      columnType: INTEGER,
      noneLabel: 'none',
      field: {
        value: '',
        onChange: sinon.spy()
      },
      fieldName: 'lower',
      style: {foo: 'bar'}
    };
  });

  it('renders', () => {
    const wrapper = shallow(<TransformRangeBound {...commonProps}/>);
    expect(wrapper.type()).to.eql('div');
    expect(wrapper.props().style).to.eql(commonProps.style);
  });

  describe('radio button state', () => {
    it('should check "none" radio when value is ""', () => {
      commonProps.field.value = '';
      const wrapper = shallow(<TransformRangeBound {...commonProps}/>);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([true, false]);
    });

    it('should check custom radios when values are not null', () => {
      const props = {
        ...commonProps,
        field: {
          value: 10,
          onChange: sinon.spy()
        }
      };
      const wrapper = shallow(<TransformRangeBound {...props}/>);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([false, true]);
    });

    it('should update checked radios when values change', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps} />);

      commonProps.field.value = '';
      wrapper.setProps(commonProps);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([true, false]);

      commonProps.field.value = 0;
      wrapper.setProps(commonProps);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([false, true]);
    });

    it('should update call bound.onChange when radio is clicked', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps}/>);
      wrapper.instance().handleRadioChange({target: {value:'none'}});
      expect(commonProps.field.onChange).to.have.been.calledWith('');
    });
    it('should run use defaultValue if value is empty', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps} />);
      wrapper.instance().handleRadioChange({ target: { value: 'notnone' }});
      expect(commonProps.field.onChange).to.have.been.calledWith(commonProps.defaultValue);
    });
  });

  describe('bounds change', () => {
    it('should update when input changes unless value is empty', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps}/>);
      wrapper.instance().handleValueChange({target:{value: 6}});
      expect(commonProps.field.onChange).to.have.been.called;
    });

    it('should update customValue but not input when value is empty', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps}/>);
      wrapper.instance().handleValueChange({target:{value: ''}});
      expect(wrapper.state('customValue')).to.eql('');
    });

    it('should update customValue when input changes and is not ""', () => {
      const wrapper = shallow(<TransformRangeBound {...commonProps}/>);

      commonProps.field.value = 6;
      wrapper.setProps(commonProps);
      expect(wrapper.state('customValue')).to.eql(6);

      commonProps.field.value = '';
      wrapper.setProps(commonProps);
      expect(wrapper.state('customValue')).to.eql(6);
    });
  });

});
