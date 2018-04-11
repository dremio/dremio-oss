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

import NewFieldSection from './NewFieldSection';

describe('NewFieldSection', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  const intialNewFieldName = 'colName';
  beforeEach(() => {
    minimalProps = {
      fields: {
        newFieldName: {
          value: intialNewFieldName,
          initialValue: intialNewFieldName,
          onChange: sinon.spy()
        },
        dropSourceField: {
          value: true,
          onChange: sinon.spy()
        }
      }
    };
    commonProps = {
      ...minimalProps,
      columnName: 'colName'
    };
    wrapper = shallow(<NewFieldSection {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<NewFieldSection {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render new field PrevalidatedTextField and drop source Checkbox', () => {
    wrapper = shallow(<NewFieldSection {...commonProps}/>);
    expect(wrapper.find('PrevalidatedTextField')).to.have.length(1);
    expect(wrapper.find('Checkbox')).to.have.length(1);
  });

  it('should not render drop source Checkbox if showDropSource = false', () => {
    wrapper = shallow(<NewFieldSection {...commonProps} showDropSource={false}/>);
    expect(wrapper.find('Checkbox')).to.have.length(0);
  });

  describe('#handleCheckboxChange', () => {
    it('should call dropSourceField.onChange', () => {
      instance.handleCheckboxChange({target: {checked: true}});
      expect(commonProps.fields.dropSourceField.onChange).to.be.calledWith();
      expect(commonProps.fields.newFieldName.onChange).to.not.be.called;
    });

    it('should append newFieldName with _1 if newFieldName has not been changed when unchecking dropSourceField',
      () => {
        instance.handleCheckboxChange({target: {checked: false}});
        expect(commonProps.fields.newFieldName.onChange).to.be.calledWith(intialNewFieldName + '_1');
      }
    );

    it('should remove _1 from newFieldName if it has not been changed when checking dropSourceField',
      () => {
        wrapper.setProps({fields: {
          ...commonProps.fields, newFieldName: {
            ...commonProps.fields.newFieldName,
            value: intialNewFieldName + '_1'
          }
        }});
        instance.handleCheckboxChange({target: {checked: true}});
        expect(commonProps.fields.newFieldName.onChange).to.be.calledWith(intialNewFieldName);
      });
  });
});
