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

import { SingleCluster } from './SingleCluster';

describe('SingleCluster', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      removeProvision: sinon.spy(),
      viewState: Immutable.fromJS({}),
      entity: Immutable.fromJS({
        clusterType: 'YARN',
        currectState: 'RUNNING',
        desiredState: 'RUNNING',
        subPropertyList: [
          { key: 'yarn.resourcemanager.hostname', value: 'yarn_host'}
        ],
        virtualCoreCount: 1,
        memoryMB: 1536
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SingleCluster {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should not render error message when it absent', () => {
    const wrapper = shallow(<SingleCluster {...minimalProps}/>);
    expect(wrapper.find('Message')).to.have.length(0);
  });

  it('should render appropriate message when cluster has an error', () => {
    const props = {
      ...minimalProps,
      entity: minimalProps.entity.merge({ error: 'Error Message',  detailedError: 'more info about error' })
    };
    const wrapper = shallow(<SingleCluster {...props}/>, context);
    const message = wrapper.find('Message');
    expect(message.props().messageType).to.be.eql('error');
    expect(message.props().message).to.be.eql(
      Immutable.Map({ errorMessage: 'Error Message', moreInfo: 'more info about error'})
    );
  });

  describe('#getIsInReadOnlyState', () => {
    let wrapper, instance;
    beforeEach(() => {
      wrapper = shallow(<SingleCluster {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('it should return true if currentState is starting', () => {
      wrapper.setProps({
        entity: minimalProps.entity.set('currentState', 'STARTING')
      });
      expect(instance.getIsInReadOnlyState()).to.be.true;
    });
    it('it should return true if currentState is stopping', () => {
      wrapper.setProps({
        entity: minimalProps.entity.set('currentState', 'STOPPING')
      });
      expect(instance.getIsInReadOnlyState()).to.be.true;
    });
    it('it should return true if desiredState is deleted', () => {
      wrapper.setProps({
        entity: minimalProps.entity.set('desiredState', 'DELETED')
      });
      expect(instance.getIsInReadOnlyState()).to.be.true;
    });
    it('it should return false if none of the above are true', () => {
      expect(instance.getIsInReadOnlyState()).to.be.false;
    });
  });

  it('should not render modification option for getIsInReadOnlyState() === true', () => {
    const wrapper = shallow(<SingleCluster {...minimalProps}/>);
    expect(wrapper.find('FontIcon[type="EditSmall"]')).to.have.length(1);

    sinon.stub(wrapper.instance(), 'getIsInReadOnlyState').returns(true);
    wrapper.setProps({
      entity: minimalProps.entity.set('desiredState', 'DELETED')
    });
    wrapper.update();
    expect(wrapper.find('FontIcon[type="EditSmall"]')).to.have.length(0);
  });

  it('should not render trash option for desiredState DELETED', () => {
    const wrapper = shallow(<SingleCluster {...minimalProps}/>);
    expect(wrapper.find('FontIcon[type="Trash"]')).to.have.length(1);

    wrapper.setProps({
      entity: minimalProps.entity.set('desiredState', 'DELETED')
    });
    wrapper.update();
    expect(wrapper.find('FontIcon[type="EditSmall"]')).to.have.length(0);
  });


  describe('#getWorkerInfoTitle', () => {
    it('should return cluster resource info', () => {
      const wrapper = shallow(<SingleCluster {...minimalProps}/>);
      const instance = wrapper.instance();
      const expectedHtml = '<span>yarn_host<span style="padding:0 .5em">|</span>'
        + '1 core, 1.5GB memory per worker</span>';
      expect(shallow(instance.getWorkerInfoTitle()).html()).to.be.eql(expectedHtml);
    });
  });
});
