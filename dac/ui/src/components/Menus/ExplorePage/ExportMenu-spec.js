/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils.js';
import config from 'dyn-load/utils/config';
import { ExportMenu } from './ExportMenu';

describe('ExportMenu', () => {
  let minimalProps;
  let commonProps;

  before(() => {
    sinon.stub(localStorageUtils, 'getAuthToken').returns('test_token');
  });

  after(() => {
    localStorageUtils.getAuthToken.restore();
  });

  beforeEach(() => {
    minimalProps = {
      action: sinon.spy(),
      jobId: 'abc123',
      addNotification: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      datasetColumns: ['TEXT']
    };
  });

  afterEach(() => {
    config.downloadRecordsLimit = 2000;
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExportMenu {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('render', () => {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<ExportMenu {...commonProps}/>);
    });

    it('should render export menu items when passed', () => {
      expect(wrapper.find('MenuItem')).to.have.length(3);
    });

    //TODO

    // it('should render menu item with href', () => {
    //   const menuItem = wrapper.find('MenuItem').at(0);
    //   expect(menuItem.props().href).to.equal('///apiv2/job/abc123/download/?downloadFormat=JSON&Authorization=test_token');
    // });

    it('should render appropriate menu item label', () => {
      expect(wrapper.find('MenuItem').at(0).children().text()).to.be.eql('JSON');
    });

    it('should render disabled menu item when dataset columns one of MAP, LIST or MIXED', () => {
      const getCsvMenuItem = () => wrapper.find('MenuItem').at(1);
      wrapper.setProps({ datasetColumns: ['MAP'] });
      expect(getCsvMenuItem().props().disabled).to.be.true;
      wrapper.setProps({ datasetColumns: ['LIST'] });
      expect(getCsvMenuItem().props().disabled).to.be.true;
      wrapper.setProps({ datasetColumns: ['MIXED'] });
      expect(getCsvMenuItem().props().disabled).to.be.true;
      getCsvMenuItem().simulate('click');
      expect(commonProps.action).to.be.not.called;
    });

    it('should render disabled menu items when sql has changed', () => {
      const getCsvMenuItem = () => wrapper.find('MenuItem').at(1);
      expect(getCsvMenuItem().props().disabled).to.be.false;
      wrapper.setProps({ datasetSql: 'select * from a', currentSql: 'select * from b'});
      expect(getCsvMenuItem().props().disabled).to.be.true;
    });
  });
});
