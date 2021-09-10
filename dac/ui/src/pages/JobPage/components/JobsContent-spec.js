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
import Immutable from 'immutable';
import socket from 'utils/socket';

import { JobState } from '@app/utils/jobsUtils';
import JobsContent from './JobsContent';

describe('JobsContent', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      onUpdateQueryState: sinon.spy(),
      loadItemsForFilter: sinon.spy(),
      loadNextJobs: sinon.spy(),
      askGnarly: sinon.spy(),
      location: {},
      queryState: Immutable.Map(),
      columnFilterSelect: sinon.spy(),
      columnFilterUnSelect: sinon.spy(),
      checkedItems: []
    };
    commonProps = {
      ...minimalProps,
      jobId: '123',
      jobs: Immutable.fromJS([{
        id: '123',
        state: 'COMPLETED'
      }])
    };
    context = {
      router: {
        push: sinon.spy(),
        replace: sinon.spy()
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JobsContent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render JobsFilters', () => {
    const wrapper = shallow(<JobsContent {...commonProps}/>, {context});
    expect(wrapper.find('JobsFilters')).to.have.length(1);
  });

  describe('#componentWillReceiveProps', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<JobsContent {...commonProps}/>, {context});
      instance = wrapper.instance();
    });

    it('should call setActiveJob if jobs have changed and there is no jobId', () => {
      sinon.stub(socket, 'startListenToJobProgress');
      sinon.spy(instance, 'setActiveJob');

      wrapper.setProps({jobId: undefined}).setContext(context);
      expect(instance.setActiveJob).to.not.be.called;

      wrapper.setProps({jobId: '456', jobs: Immutable.fromJS([{id: '456', state: 'RUNNING'}])})
        .setContext(context);
      expect(instance.setActiveJob).to.not.be.called;

      wrapper.setProps({jobId: undefined, jobs: Immutable.fromJS([{id: '789', state: 'RUNNING'}])})
        .setContext(context);
      expect(instance.setActiveJob).to.be.calledWith(Immutable.fromJS({id: '789', state: 'RUNNING'}), true);
      socket.startListenToJobProgress.restore();
    });

    it('should call setActiveJob with first job if jobs have changed and there is no active job id', () => {
      sinon.spy(instance, 'setActiveJob');
      sinon.stub(instance, 'runActionForJobs');

      wrapper.setProps({jobId: undefined, jobs: Immutable.fromJS([{id: '456', state: 'RUNNING'}])})
        .setContext(context);
      expect(instance.setActiveJob).to.be.calledWith(Immutable.fromJS({id: '456', state: 'RUNNING'}), true);
    });

    it('should call runActionForJobs with startListenToJob as a callback when jobs change', () => {
      sinon.stub(socket, 'startListenToJobProgress');
      sinon.spy(instance, 'runActionForJobs');

      wrapper.setProps(commonProps).setContext(context);
      const jobs = Immutable.fromJS([{id: '456', state: 'RUNNING'}]);
      expect(instance.runActionForJobs).to.not.be.called;
      wrapper.setProps({jobId: '456', jobs}).setContext(context);
      expect(instance.runActionForJobs).to.be.calledOnce;
      expect(instance.runActionForJobs).to.be.calledWith(jobs, false);
      expect(socket.startListenToJobProgress).to.be.calledWith(jobs.getIn([0, 'id']));
      socket.startListenToJobProgress.restore();
    });
  });

  describe('#runActionForJobs', () => {
    it('should call callback for any running job', () => {
      const props = {
        ...commonProps,
        jobId: '456',
        jobs: Immutable.fromJS([{id: '456', state: 'RUNNING'}])
      };
      const instance = shallow(<JobsContent {...props}/>, {context}).instance();

      const callbackList = [];
      const callback = (id) => {
        callbackList.push(id);
      };

      const jobs = Immutable.fromJS([
        {id: '1', state: JobState.COMPLETED},
        {id: '2', state: JobState.FAILED},
        {id: '3', state: JobState.CANCELED},
        {id: '4', state: JobState.CANCELLATION_REQUESTED},
        {id: '5', state: JobState.ENQUEUED},
        {id: '6', state: JobState.STARTING},
        {id: '7', state: JobState.RUNNING},
        {id: '8', state: JobState.PLANNING},
        {id: '9', state: JobState.NOT_SUBMITTED}
      ]);

      instance.runActionForJobs(jobs, false, callback);

      expect(callbackList).to.eql(['4', '5', '6', '7', '8', '9']);
    });
  });

  describe('#componentWillUnmount', () => {
    it('should call runActionForJobs with stopListenToJob as a callback', () => {
      const props = {
        ...commonProps,
        jobId: '456',
        jobs: Immutable.fromJS([{id: '456', state: 'RUNNING'}])
      };
      const instance = shallow(<JobsContent {...props}/>, {context}).instance();

      sinon.spy(socket, 'stopListenToJobProgress');
      sinon.spy(instance, 'runActionForJobs');

      expect(instance.runActionForJobs).to.not.be.called;

      instance.componentWillUnmount();

      expect(instance.runActionForJobs).to.be.calledOnce;
      expect(instance.runActionForJobs).to.be.calledWith(props.jobs, true);
      expect(socket.stopListenToJobProgress).to.be.calledWith(props.jobs.getIn([0, 'id']));
      socket.stopListenToJobProgress.restore();
    });
  });
});
