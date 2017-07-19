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

import ApiUtils from 'utils/apiUtils/apiUtils';
import NAS from 'components/sourceForms/NAS';
import { getResponseForEntity } from 'testUtil';

import { AddSourceModal } from './AddSourceModal';

describe('AddSourceModal', () => {

  const selectedSource = {label: 'NAS', sourceType: 'NAS'};

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      location: {
        pathname: 'somePathname',
        state: {}
      },
      hide: sinon.spy(),
      showConfirmationDialog: sinon.spy(),
      updateFormDirtyState: sinon.spy(),
      createSource: sinon.stub().returns(Promise.resolve(
        getResponseForEntity('source', 'someId', {
          id: 'someId',
          links: {
            self: 'someUrl'
          }
        })
      ))
    };
    commonProps = {
      ...minimalProps,
      isOpen: true
    };
    context = {
      router: {
        push: sinon.spy()
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AddSourceModal {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectSourceType when no source type selected', () => {
    const wrapper = shallow(<AddSourceModal {...commonProps}/>, {context});
    expect(wrapper.find('SelectSourceType')).to.have.length(1);
    wrapper.setProps({source: selectedSource});
    expect(wrapper.find('SelectSourceType')).to.have.length(0);
  });

  it('should render NAS form when source is nas', () => {
    const wrapper = shallow(<AddSourceModal {...commonProps} source={selectedSource}/>, {context});
    expect(wrapper.find(NAS)).to.have.length(1);
  });

  it('should continue to show NAS form while modal is hiding (source prop is removed)', () => {
    const wrapper = shallow(<AddSourceModal {...commonProps} source={selectedSource}/>, {context});
    wrapper.setProps({source: undefined});
    expect(wrapper.find(NAS)).to.have.length(1);
  });

  describe('handleSelectSource', () => {
    it('should call router.push with sourceType added to location.state', () => {
      const wrapper = shallow(<AddSourceModal {...commonProps}/>, {context});
      wrapper.instance().handleSelectSource(selectedSource);
      expect(context.router.push.args[0][0].state).to.eql({...commonProps.location.state, source: selectedSource});
    });
  });

  describe('submit', () => {
    let instance;
    let formValues;
    beforeEach(() => {
      sinon.spy(ApiUtils, 'attachFormSubmitHandlers');
      formValues = {
        name: 'someName',
        metadataPolicy: {
          namesRefreshMillis: {},
          datasetDefinitionTTLMillis: {},
          authTTLMillis: {}
        }
      };
      const wrapper = shallow(<AddSourceModal {...commonProps}  source={selectedSource}/>, {context});
      instance = wrapper.instance();
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });
    it('should call mutateFormValues and createSource', () => {
      sinon.spy(instance, 'mutateFormValues');
      instance.submit(formValues);
      expect(instance.mutateFormValues).to.be.calledOnce;
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
      expect(commonProps.createSource).to.be.calledOnce;
    });

    it('should call router.push with new source\'s url', () => {
      return instance.submit(formValues).then(() => {
        expect(context.router.push).to.be.calledWith('someUrl');
      });
    });
  });

  describe('#startTrackSubmitTime', () => {
    beforeEach(function() {
      this.clock = sinon.useFakeTimers();
    });
    afterEach(function() {
      this.clock.restore();
    });

    it('isSubmitTakingLong should be true after 5 seconds', function() {
      const instance = shallow(<AddSourceModal {...commonProps}  source={selectedSource}/>, {context}).instance();
      instance.startTrackSubmitTime();
      this.clock.tick(5000);
      expect(instance.state.isSubmitTakingLong).to.be.true;
      expect(instance.state.submitTimer).to.not.be.null;
    });
  });

  describe('#stopTrackSubmitTime', () => {
    it('isSubmitTakingLong should be false and reset submitTimer', () => {
      const instance = shallow(<AddSourceModal {...commonProps}  source={selectedSource}/>, {context}).instance();
      instance.setState({
        isSubmitTakingLong: true
      });
      instance.stopTrackSubmitTime();
      expect(instance.state.isSubmitTakingLong).to.be.false;
      expect(instance.state.submitTimer).to.be.null;
    });
  });
});
