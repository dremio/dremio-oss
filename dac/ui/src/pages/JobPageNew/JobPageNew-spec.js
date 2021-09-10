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
import sinon from 'sinon';
import Immutable from 'immutable';

import JobPage from './JobPageNew';

describe('JobPage', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      location: { query: { filters: {} } },
      filterJobsListData: sinon.stub(),
      updateQueryState: sinon.stub(),
      queryState: new Immutable.Map(),
      jobs: new Immutable.List()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JobPage {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
});
