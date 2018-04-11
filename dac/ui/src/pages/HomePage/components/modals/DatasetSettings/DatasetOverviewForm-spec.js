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

import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import { FormBody, FormTitle } from 'components/Forms';
import DatasetOverviewForm from './DatasetOverviewForm';

describe('DatasetOverviewForm', () => {
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetOverviewForm />);
    expect(wrapper).to.have.length(1);
  });

  it('should return null if we have not entity', () => {
    const wrapper = shallow(<DatasetOverviewForm/>);
    expect(wrapper.children().length).to.equal(0);
  });

  it('should render FormBody, FormTitle, DatasetItemLabel if we have entity', () => {
    const wrapper = shallow(<DatasetOverviewForm entity={Immutable.Map({entityType: 'VIRTUAL_DATASET'})}/>);
    expect(wrapper.find(DatasetItemLabel)).to.have.length(1);
    expect(wrapper.find(FormBody)).to.have.length(1);
    expect(wrapper.find(FormTitle)).to.have.length(1);
  });
});
