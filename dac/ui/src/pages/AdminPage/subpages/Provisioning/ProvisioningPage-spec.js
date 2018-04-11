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

import { ProvisioningPage } from './ProvisioningPage';

describe('ProvisioningPage', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      provisions: Immutable.List(),
      viewState: Immutable.Map(),
      loadProvision: sinon.stub().returns(Promise.resolve())
    };
    commonProps = {
      ...minimalProps,
      removeProvision: sinon.stub().returns(Promise.resolve()),
      editProvision: sinon.stub().returns(Promise.resolve()),
      showConfirmationDialog: sinon.spy(),
      openEditProvisionModal: sinon.spy(),
      provisions: Immutable.fromJS([
        {
          id: '1',
          clusterType: 'YARN'
        },
        {
          id: '2',
          clusterType: 'YARN'
        }
      ])
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ProvisioningPage {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  it('should render list of available provision managers when no manager created', () => {
    const wrapper = shallow(<ProvisioningPage {...minimalProps}/>);
    expect(wrapper.find('ClusterListView')).to.have.length(0);
    expect(wrapper.find('SelectClusterType')).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  it('should render manager info when any created', () => {
    const wrapper = shallow(<ProvisioningPage {...commonProps}/>);
    expect(wrapper.find('SelectClusterType')).to.have.length(0);
    expect(wrapper.find('ClusterListView')).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  describe('#componentWillMount', () => {
    it('should call startPollingProvisionData and with isFirst=true', () => {
      const wrapper = shallow(<ProvisioningPage {...commonProps}/>);
      const instance = wrapper.instance();
      sinon.spy(instance, 'startPollingProvisionData');
      instance.componentWillMount();
      expect(instance.startPollingProvisionData).to.be.calledOnce;
      expect(instance.startPollingProvisionData).to.be.calledWith(true);
      instance.componentWillUnmount();
    });
  });

  describe('#componentWillUnmount', () => {
    it('should call stopPollingProvisionData', () => {
      const wrapper = shallow(<ProvisioningPage {...commonProps}/>);
      const instance = wrapper.instance();
      sinon.spy(instance, 'stopPollingProvisionData');
      instance.componentWillUnmount();
      expect(instance.stopPollingProvisionData).to.be.calledOnce;
      instance.componentWillUnmount();
    });
  });

  describe('#startPollingProvisionData', () => {
    let instance;
    let clock;
    beforeEach(() => {
      instance = shallow(<ProvisioningPage {...commonProps}/>).instance();

      sinon.spy(instance, 'startPollingProvisionData');
      clock = sinon.useFakeTimers();
    });
    afterEach(() => {
      instance.startPollingProvisionData.restore();
      clock.restore();
    });
    after(() => {
      instance.componentWillUnmount();
    });

    it('should call startPollingProvisionData again after timeout', () => {
      instance.startPollingProvisionData();
      clock.tick(3000);
      commonProps.loadProvision().then(() => {
        expect(instance.startPollingProvisionData).to.be.calledTwice;
      });
    });
    it('should not call startPollingProvisionData again if poll was canceled', () => {
      instance.startPollingProvisionData();
      instance.stopPollingProvisionData();
      clock.tick(3000);
      commonProps.loadProvision().then(() => {
        expect(instance.startPollingProvisionData).to.be.calledOnce;
      });
    });
    it('should call startPollingProvisionData again if it was called for the first time', () => {
      instance.pollId = undefined;
      instance.startPollingProvisionData(true);
      clock.tick(3000);
      commonProps.loadProvision().then(() => {
        expect(instance.startPollingProvisionData).to.be.calledTwice;
      });
    });
    it('should not call startPollingProvisionData again if it was called not for the first time', () => {
      instance.pollId = undefined;
      instance.startPollingProvisionData();
      clock.tick(3000);
      commonProps.loadProvision().then(() => {
        expect(instance.startPollingProvisionData).to.be.calledOnce;
      });
    });
  });

  describe('#stopPollingProvisionData', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<ProvisioningPage {...commonProps}/>).instance();
    });
    afterEach(() => {
      instance.componentWillUnmount();
    });
    it('should reset pollId to undefined', () => {
      expect(instance.pollId).to.be.not.undefined;
      instance.stopPollingProvisionData();
      expect(instance.pollId).to.be.undefined;
    });
  });

  describe('#handleRemoveProvision', () => {
    it('should show confirmation dialog before remove', () => {
      const instance = shallow(<ProvisioningPage {...commonProps} />).instance();
      sinon.spy(instance, 'removeProvision');
      instance.handleRemoveProvision();
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(instance.removeProvision).to.not.be.called;
      instance.componentWillUnmount();
    });

    it('should call remove provision when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const instance = shallow(<ProvisioningPage {...props} />).instance();
      sinon.spy(instance, 'removeProvision');
      instance.handleRemoveProvision(Immutable.fromJS({id: 1}));
      expect(instance.removeProvision).to.be.called;
      expect(props.removeProvision).to.be.called;
      instance.componentWillUnmount();
    });
  });

  describe('#handleEditProvision', () => {
    it('should call openEditProvisionModal', () => {
      const instance = shallow(<ProvisioningPage {...commonProps} />).instance();
      instance.handleEditProvision(commonProps.provisions.get(0));
      expect(commonProps.openEditProvisionModal).to.be.calledWith('1', 'YARN');
      instance.componentWillUnmount();
    });
  });

  describe('#handleStopProvision', () => {
    it('should show confirmation dialog before stop cluster', () => {
      const instance = shallow(<ProvisioningPage {...commonProps} />).instance();
      instance.handleStopProvision(sinon.stub());
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.editProvision).to.not.be.called;
      instance.componentWillUnmount();
    });

    it('should call stop provision when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const instance = shallow(<ProvisioningPage {...props} />).instance();
      const stub = sinon.stub();
      instance.handleStopProvision(stub);
      expect(stub).to.be.called;
      instance.componentWillUnmount();
    });
  });

  describe('#handleChangeProvisionState', () => {
    let instance;
    const fakeCluster = Immutable.Map({
      id: 'foo',
      version: '1',
      workersSummary: 'foo',
      currentState: 'foo',
      containers: 'foo',
      error: 'foo'
    });
    beforeEach(() => {
      instance = shallow(<ProvisioningPage {...commonProps} />).instance();
      sinon.spy(instance, 'handleStopProvision');
    });
    afterEach(() => {
      instance.componentWillUnmount();
    });

    it('should call handleStopProvision if desiredState is STOPPED', () => {
      instance.handleChangeProvisionState('STOPPED', fakeCluster);
      expect(instance.handleStopProvision).to.be.called;
      expect(commonProps.editProvision).to.not.be.called;
    });

    it('should call editProvision if desiredState is RUNNING', () => {
      instance.handleChangeProvisionState('RUNNING', fakeCluster);
      expect(instance.handleStopProvision).to.not.be.called;
      expect(commonProps.editProvision).to.be.calledWith({ desiredState: 'RUNNING', id: 'foo', version: '1' });
    });
  });
});
