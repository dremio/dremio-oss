/*
 * Copyright (C) 2017 Dremio Corporation
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
import { shallow, mount } from 'enzyme';
import ValueList, { ValueItem } from './ValueList';

describe('ValueList', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      fieldList: [
        {
          value: 'foo'
        },
        {
          value: 'bar'
        }
      ]
    };
    commonProps.fieldList.addField = sinon.spy();
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ValueList {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render FieldList, FieldWithError, TextField, SimpleButton', () => {
    const wrapper = shallow(<ValueList {...commonProps}/>);
    expect(wrapper.find('FieldList')).to.have.length(1);
    expect(wrapper.find('FieldWithError')).to.have.length(1);
    expect(wrapper.find('TextField')).to.have.length(1);
    expect(wrapper.find('SimpleButton')).to.have.length(1);
  });

  describe('#onInputChange', () => {
    it('should set inputValue with new value', () => {
      const wrapper = shallow(<ValueList {...commonProps}/>);
      const instance = wrapper.instance();
      const e = {
        target: {
          value: 'foo'
        }
      };
      instance.onInputChange(e);
      expect(wrapper.state('inputValue')).to.eql(e.target.value);
    });
  });

  describe('#addItem', () => {
    let e;
    let wrapper;
    let instance;
    beforeEach(() => {
      e = {
        preventDefault: sinon.spy()
      };
      wrapper = shallow(<ValueList {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should set inputError to "Enter a value" string if inputValue not defined', () => {
      instance.addItem(e);
      expect(wrapper.state('inputError')).to.eql('Enter a value');
    });
    it('should set inputError to "Already added" string if value is already exist', () => {
      wrapper.setState({
        inputValue: 'foo'
      });
      instance.addItem(e);
      expect(wrapper.state('inputError')).to.eql('Already added');
    });
    it('should call fieldList.addField and reset components state', () => {
      wrapper.setState({
        inputValue: 'baz'
      });
      instance.addItem(e);
      expect(commonProps.fieldList.addField).to.be.calledWith('baz');
      expect(wrapper.state('inputValue')).to.be.eql('');
    });
  });

  describe('ValueItem', () => {
    let minimalPropsItem;
    let commonPropsItem;
    beforeEach(() => {
      minimalPropsItem = {
        item: {
          value: '123'
        }
      };
      commonPropsItem = {
        ...minimalPropsItem,
        onRemove: sinon.spy()
      };
    });
    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<ValueItem {...minimalPropsItem}/>);
      expect(wrapper).to.have.length(1);
    });
    it('should render RemoveButton only if onRemove is defined', () => {
      const wrapper = shallow(<ValueItem {...commonPropsItem}/>);
      expect(wrapper.find('RemoveButton')).to.have.length(1);

      wrapper.setProps({onRemove: undefined});
      expect(wrapper.find('RemoveButton')).to.have.length(0);
    });

    describe('#componentDidMount', () => {
      let instance;
      beforeEach(() => {
        instance = mount(<ValueItem {...commonPropsItem}/>).instance();
        sinon.spy(instance, 'showTooltip');
      });
      afterEach(() => {
        instance.showTooltip.restore();
      });
      it('should call showTooltip if greaterThanMaxWidth returns true', () => {
        sinon.stub(instance, 'greaterThanMaxWidth').returns(true);

        instance.componentDidMount();

        expect(instance.showTooltip.called).to.be.true;
      });
      it('should not call showTooltip if greaterThanMaxWidth returns false', () => {
        sinon.stub(instance, 'greaterThanMaxWidth').returns(false);

        instance.componentDidMount();

        expect(instance.showTooltip.called).to.be.false;
      });
    });

    describe('#showTooltip', () => {
      let wrapper;
      let instance;
      beforeEach(() => {
        wrapper = shallow(<ValueItem {...commonPropsItem}/>);
        instance = wrapper.instance();
      });
      it('should set state with textWidth', () => {
        instance.showTooltip(200);

        expect(wrapper.state('textWidth')).to.eql(200);
      });
    });
  });
});
