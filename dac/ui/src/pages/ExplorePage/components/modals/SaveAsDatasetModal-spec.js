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
import SaveAsDatasetForm from 'pages/ExplorePage/components/forms/SaveAsDatasetForm';
import { NEXT_ACTIONS } from 'actions/explore/nextAction';
import ApiUtils from 'utils/apiUtils/apiUtils';

import { SaveAsDatasetModal } from './SaveAsDatasetModal';

describe('SaveAsDatasetModal', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      viewState: Immutable.Map(),
      saveAsDataset: sinon.spy(),
      loadDependentDatasets: sinon.spy(),
      submitReapplyAndSaveAsDataset: sinon.stub().returns(Promise.resolve({payload: Immutable.Map()})),
      submitSaveAsDataset: sinon.stub().returns(Promise.resolve({payload: Immutable.Map()})),
      navigateToNextDataset: sinon.stub().returns(Promise.resolve('navigateToNextDataset')),
      afterSaveDataset: sinon.stub().returns(Promise.resolve('afterSaveDataset'))
    };

    commonProps = {
      ...minimalProps,
      isOpen: true,
      hide: sinon.spy(),
      location: {pathname: 'foopath'}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SaveAsDatasetModal {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Modal and Form', () => {
    const wrapper = shallow(<SaveAsDatasetModal {...commonProps}/>);

    const modalProps = wrapper.find('Modal').props();
    expect(modalProps.isOpen).to.equal(commonProps.isOpen);
    expect(modalProps.hide).to.equal(commonProps.hide);

    const formProps = wrapper.find(SaveAsDatasetForm).props();
    expect(formProps.onFormSubmit).to.equal(wrapper.instance().submit);
    expect(formProps.onCancel).to.equal(commonProps.hide);
    expect(wrapper.find(SaveAsDatasetForm).props().message).to.be.undefined;
  });

  it('should set message if nextAction is tableau or qlik', () => {
    const wrapper = shallow(<SaveAsDatasetModal {...commonProps} nextAction={NEXT_ACTIONS.openTableau}/>);
    expect(wrapper.find(SaveAsDatasetForm).props().message).to.contain('Tableau');

    const wrapper2 = shallow(<SaveAsDatasetModal {...commonProps} nextAction={NEXT_ACTIONS.openQlik}/>);
    expect(wrapper2.find(SaveAsDatasetForm).props().message).to.contain('Qlik');
  });

  describe('submit', () => {
    it('dispatches submitSaveAsDataset then afterSaveDataset', () => {
      sinon.spy(ApiUtils, 'attachFormSubmitHandlers');
      const wrapper = shallow(<SaveAsDatasetModal {...commonProps}/>);
      const instance = wrapper.instance();
      const promise = instance.submit({name: 'name', location: 'location'});
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
      ApiUtils.attachFormSubmitHandlers.restore();
      return expect(promise).to.eventually.eql('afterSaveDataset');
    });

    it('dispatches submitReapplyAndSaveAsDataset when reapply = "ORIGINAL"', () => {
      const wrapper = shallow(<SaveAsDatasetModal {...commonProps}/>);
      const instance = wrapper.instance();
      const promise = expect(
        instance.submit({name: 'name', location: 'location', reapply: 'ORIGINAL'})
      );
      expect(commonProps.submitReapplyAndSaveAsDataset).to.have.been.called;
      return promise.to.eventually.eql('afterSaveDataset');
    });
  });

  describe('componentWillReceiveProps', () => {
    let nextProps;
    beforeEach(() => {
      nextProps = {
        dataset: Immutable.fromJS({'fullPath': null}),
        loadDependentDatasets: sinon.spy()
      };
    });

    it('shouldn\'t run loadDependentDatasets when popup should be closed ', () => {
      const instance = shallow(<SaveAsDatasetModal {...commonProps} isOpen/>).instance();
      nextProps.isOpen = false;
      instance.componentWillReceiveProps(nextProps);
      expect(nextProps.loadDependentDatasets.called).to.be.false;
    });
    it('shouldn\'t run loadDependentDatasets when popup was already opened ', () => {
      const instance = shallow(<SaveAsDatasetModal {...commonProps} isOpen/>).instance();
      nextProps.isOpen = true;
      instance.componentWillReceiveProps(nextProps);
      expect(nextProps.loadDependentDatasets.called).to.be.false;
    });
    it('should run loadDependentDatasets when popup should be opened ', () => {
      const instance = shallow(<SaveAsDatasetModal {...commonProps} isOpen={false}/>).instance();
      nextProps.isOpen = true;
      instance.componentWillReceiveProps(nextProps);
      expect(nextProps.loadDependentDatasets.called).to.be.true;
    });
  });
});
