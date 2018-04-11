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
import Immutable from 'immutable';
import { ClusterWorkers } from './ClusterWorkers';

describe('ClusterWorkers', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      entity: Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'RUNNING',
        desiredState: 'RUNNING',
        version: '1',
        id: 'foo'
      })
    };
    commonProps = {
      ...minimalProps,
      changeProvisionState: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ClusterWorkers {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#toggleClusterState', () => {
    it('should call changeProvisionState with STOPPED if currentState is RUNNING', () => {
      const instance = shallow(<ClusterWorkers {...commonProps}/>).instance();
      instance.toggleClusterState();
      expect(commonProps.changeProvisionState).to.be.calledWith('STOPPED', commonProps.entity);
    });

    it('should call changeProvisionState with RUNNING if currentState is STOPPED', () => {
      const wrapper = shallow(<ClusterWorkers {...commonProps}/>);
      const entity = commonProps.entity.set('currentState', 'STOPPED');
      wrapper.setProps({entity});
      wrapper.instance().toggleClusterState();
      expect(commonProps.changeProvisionState).to.be.calledWith('RUNNING', entity);
    });
  });

  describe('#render', () => {
    it('if currentState is RUNNING', () => {
      const wrapper = shallow(<ClusterWorkers {...commonProps}/>);
      expect(wrapper.find('Button').get(0).props.text).to.equal('Stop');
      expect(wrapper.text().includes('Status: Running')).to.be.true;
    });

    it('if currentState is STOPPED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'STOPPED',
        desiredState: 'STOPPED'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button').get(0).props.text).to.equal('Start');
      expect(wrapper.text()).to.include('Status: Stopped');
    });

    it('if currentState is FAILED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'FAILED',
        desiredState: 'RUNNING'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button').get(0).props.text).to.equal('Start');
      expect(wrapper.text()).to.include('Target: Running (Currently: Failed)');
    });

    it('if currentState is CREATED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'CREATED',
        desiredState: 'RUNNING'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Target: Running (Currently: Created)');
    });

    it('if currentState is DELETED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'DELETED',
        desiredState: 'DELETED'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Status: Deleted');
    });

    it('if currentState is STOPPING and desiredState DELETED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'STOPPING',
        desiredState: 'DELETED'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Status: Deleting');
    });

    it('if currentState is STOPPING and desiredState RUNNING', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'STOPPING',
        desiredState: 'RUNNING'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Status: Restarting');
    });

    it('if currentState is STARTING and desiredState RUNNING', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'STARTING',
        desiredState: 'RUNNING'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Status: Starting');
    });

    it('if currentState is STOPPING and desiredState STOPPED', () => {
      const entity = Immutable.fromJS({
        workersSummary: { total: 2 },
        currentState: 'STOPPING',
        desiredState: 'STOPPED'
      });
      const wrapper = shallow(<ClusterWorkers {...commonProps} entity={entity}/>);
      expect(wrapper.find('Button')).to.have.length(1);
      expect(wrapper.text()).to.include('Status: Stopping');
    });

  });
});
