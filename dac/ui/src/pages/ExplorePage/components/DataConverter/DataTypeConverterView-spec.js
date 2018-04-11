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
import { TEXT, BINARY, INTEGER } from 'constants/DataTypes';
import Tabs from 'components/Tabs';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import DataTypeConverterView from './DataTypeConverterView';

import NoParamForm from './forms/NoParamForm';
import NonMatchingForm from './forms/NonMatchingForm';

describe('DataTypeConverterView', () => {
  let commonProps;
  let commonContext;
  beforeEach(() => {
    commonProps = {
      columnName: 'a',
      columnType: TEXT,
      submit: sinon.spy(),
      cancel: sinon.spy()
    };
    commonContext = {
      router: { push: sinon.spy() },
      location: {
        state: {
          toType: INTEGER
        }
      }
    };
  });
  it('should render', () => {
    const wrapper = shallow(<DataTypeConverterView {...commonProps} />, { context: commonContext });
    expect(wrapper).to.have.length(1);
  });
  it('should render NonMatchingForm for TEXT -> INTEGER', () => {
    const wrapper = shallow(<DataTypeConverterView {...commonProps} />, { context: commonContext });
    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql(TEXT);
    expect(wrapper.find(NonMatchingForm)).to.exist;
    expect(wrapper.find(NonMatchingForm).at(0).prop('tabId')).to.eql(INTEGER);
  });
  it('should render NoParamForm for TEXT -> BINARY', () => {
    const context = {
      ...commonContext,
      location: {
        state: {
          toType: BINARY
        }
      }
    };
    const wrapper = shallow(<DataTypeConverterView {...commonProps} />, { context });

    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql(TEXT);
    expect(wrapper.find(Tabs).at(1).prop('activeTab')).to.eql(BINARY);
    expect(wrapper.find(NoParamForm)).to.exist;
    expect(wrapper.find(NoParamForm).at(0).prop('tabId')).to.eql(BINARY);
  });
  it('should render Select with value "Select Type" only if toType is not defined', () => {
    const context = {
      ...commonContext,
      location: {
        state: {
          toType: null
        }
      }
    };
    const wrapper = shallow(<DataTypeConverterView {...commonProps} />, { context });
    let select = wrapper.find('Select');

    expect(select).to.have.length(1);
    expect(select.prop('value')).to.eql('Select Type');

    wrapper.setContext(commonContext);
    select = wrapper.find('Select');

    expect(select.prop('value')).to.eql(commonContext.location.state.toType);
  });

  describe('#setTransformationInLocalStorage', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<DataTypeConverterView {...commonProps}/>, { context: commonContext });
      instance = wrapper.instance();
      sinon.stub(localStorageUtils, 'setTransformValue');
    });
    afterEach(() => {
      localStorageUtils.setTransformValue.restore();
    });

    it('should save all fields except newFieldName', () => {
      const values = {
        newFieldName: 'abs',
        dropSourceField: true,
        rounding: 'ROUND',
        delimiter: ','
      };
      instance.setTransformationInLocalStorage(values);
      expect(localStorageUtils.setTransformValue).to.be.calledWith({
        dropSourceField: true,
        rounding: 'ROUND',
        delimiter: ','
      });
    });
  });
});
