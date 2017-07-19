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
import { shallow } from 'enzyme';
import { Link } from 'react-router';

import DatasetOverviewFormBase from 'pages/HomePage/components/modals/DatasetSettings/DatasetOverviewForm';
import DatasetOverviewFormMixin from './DatasetOverviewFormMixin';

@DatasetOverviewFormMixin
class DatasetOverviewForm extends DatasetOverviewFormBase {}

describe('DatasetOverviewFormMixin', () => {
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetOverviewForm />);
    expect(wrapper).to.have.length(1);
  });

  it('should render move link only if entityType VIRTUAL_DATASET', () => {
    const wrapper = shallow(<DatasetOverviewForm />);
    wrapper.setProps({
      entity: Immutable.Map({ entityType: 'VIRTUAL_DATASET' })
    });
    expect(wrapper.find(Link)).to.have.length(1);

    wrapper.setProps({
      entity: Immutable.Map({ entityType: 'file' })
    });
    expect(wrapper.find(Link)).to.have.length(0);

    wrapper.setProps({
      entity: Immutable.Map({ entityType: 'folder' })
    });
    expect(wrapper.find(Link)).to.have.length(0);

    wrapper.setProps({
      entity: Immutable.Map({ entityType: 'physicalDataset' })
    });
    expect(wrapper.find(Link)).to.have.length(0);
  });
});
