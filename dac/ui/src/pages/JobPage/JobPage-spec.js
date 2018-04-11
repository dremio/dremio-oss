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
import sinon from 'sinon';
import Immutable from 'immutable';

import { JobPage } from './JobPage';

describe('JobPage', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      location: {query:{filters:{}}},
      filterJobsData: sinon.stub(),
      updateQueryState: sinon.stub(),
      queryState: new Immutable.Map(),
      jobs: new Immutable.List()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JobPage {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  describe('#receiveProps', () => {
    it('should updateQueryState if location.query is empty', () => {
      const instance = shallow(<JobPage {...minimalProps}/>).instance();
      instance.receiveProps({...minimalProps, location: {query:{}}});
      expect(minimalProps.filterJobsData).to.have.not.been.called;
      expect(minimalProps.updateQueryState.getCall(0).args[0].toJS()).to.eql({filters:{qt:['UI', 'EXTERNAL']}});
    });

    it('should filterJobsData if queryState changed', () => {
      const instance = shallow(<JobPage {...minimalProps}/>).instance();
      const newQS = new Immutable.Map({foo: 1});
      instance.receiveProps({...minimalProps, queryState: newQS});
      expect(minimalProps.filterJobsData).to.have.been.calledWith(newQS, 'JOB_PAGE_VIEW_ID');
      expect(minimalProps.updateQueryState).to.have.not.been.called;
    });

    it('should do nothing by default', () => {
      const instance = shallow(<JobPage {...minimalProps}/>).instance();
      instance.receiveProps(minimalProps, minimalProps);
      expect(minimalProps.filterJobsData).to.have.not.been.called;
      expect(minimalProps.updateQueryState).to.have.not.been.called;
    });
  });
});
