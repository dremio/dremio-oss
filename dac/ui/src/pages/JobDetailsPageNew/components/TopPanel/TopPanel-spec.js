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
import { TopPanel } from './TopPanel';

describe('TopPanel', () => {

  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      changePages: sinon.spy(),
      jobId: '456',
      jobStatus:'',
      router: {},
      location: {},
      jobDetails: Immutable.fromJS({
        attemptDetails: [{
          profileUrl: ''
        }],
        queriedDatasets: [{datasetPathsList: []}]
      })
    };
  });

  xit('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TopPanel {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  xit('should render jobid', () => {
    const props = {
      ...minimalProps,
      jobId: '293'
    };
    const wrapper = shallow(<TopPanel {...props} />);
    const element = wrapper.find('[data-qa="top-panel-jobId"]');
    expect(element.text()).to.equal('293');
  });
});
