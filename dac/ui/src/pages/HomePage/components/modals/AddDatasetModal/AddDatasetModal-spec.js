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

import AddDatasetController from './AddDatasetController';

import AddDatasetModal from './AddDatasetModal';

describe('AddDatasetModal', () => {

  const commonProps = {
    query: {},
    isOpen: false,
    hide: sinon.spy(),
    pathname: 'pathname'
  };

  const context = {
    location: { query: 'ds1' },
    routeParams: {resourceId: 'name.name'}
  };

  it('should render AddDatasetController', () => {
    const wrapper = shallow(<AddDatasetModal {...commonProps}/>, {context});
    expect(wrapper.find(AddDatasetController)).to.have.length(1);
  });

  it('should pass props to AddDatasetController', () => {
    const wrapper = shallow(<AddDatasetModal {...commonProps}/>, {context});
    const res = wrapper.find(AddDatasetController).props();
    expect(res.hide).to.eql(wrapper.instance().props.hide);
    expect(res.location).to.eql(context.location);
    expect(res.pathname).to.eql('pathname');
    expect(res.routeParams).to.eql({resourceId: 'name.name'});
  });

});
