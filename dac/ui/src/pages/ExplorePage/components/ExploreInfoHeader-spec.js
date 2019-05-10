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

import { NEXT_ACTIONS } from 'actions/explore/nextAction';
import { ExploreInfoHeader } from './ExploreInfoHeader';

describe('ExploreInfoHeader', () => {

  let commonProps;
  let context;
  let wrapper;
  let instance;
  beforeEach(() => {
    commonProps = {
      pageType: 'default',
      exploreViewState: Immutable.fromJS({}),
      dataset: Immutable.fromJS({
        datasetVersion: '11',
        tipVersion: '22',
        sql: '23',
        fullPath: ['newSpace', 'newTable'],
        displayFullPath: ['displaySpace', 'displayTable'],
        datasetType: 'VIRTUAL_DATASET'
      }),
      currentSql: '12',
      queryContext: Immutable.List(),
      routeParams: {
        tableId: 'newTable',
        resourceId: 'newSpace',
        resources: 'space'
      },
      location: {
        pathname: 'ds1',
        query: {
          tipVersion: '22',
          version: '22',
          mode: 'edit'
        }
      },
      tableColumns: Immutable.fromJS([
        { type: 'INTEGER'},
        { type: 'TEXT'}
      ]),
      saveDataset: sinon.stub().returns(Promise.resolve('saveAsDataset')),
      saveAsDataset: sinon.stub().returns(Promise.resolve('saveAsDataset')),
      runTableTransform: sinon.stub().returns(Promise.resolve('runTableTransform')),
      performNextAction: sinon.stub().returns(Promise.resolve('performNextAction')),
      performTransformAndRun: sinon.spy(),
      startDownloadDataset: sinon.spy(),
      toggleRightTree: sinon.spy(),
      runDataset: sinon.spy(),
      runDatasetSql: sinon.spy(),
      previewDatasetSql: sinon.spy(),
      performTransform: sinon.stub().returns(Promise.resolve('performTransform')),
      transformHistoryCheck: sinon.spy(),
      performLoadDataset: sinon.stub().returns(Promise.resolve('performLoadDataset')),
      navigateToNextDataset: sinon.stub().returns('navigateToNextDataset')
    };
    context = {
      router : {push: sinon.spy()},
      routeParams: {
        tableId: 'newTable',
        resourceId: 'newSpace',
        resources: 'space'
      },
      location: {
        pathname: 'pathname',
        query: {
          version: '123456',
          mode: 'edit'
        }
      }
    };
    wrapper = shallow(<ExploreInfoHeader {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  describe('rendering', () => {
    it('should render .explore-info-header', () => {
      expect(wrapper.hasClass('explore-info-header')).to.equal(true);
    });
  });


  it('run and save buttons should be enabled', () => {
    expect(wrapper.find('.run-button').length).to.equal(1);
    expect(wrapper.find('.explore-save-button').length).to.equal(1);
    expect(wrapper.find('.run-button').getElement().props.disabled).to.be.undefined;
    expect(wrapper.find('.explore-save-button').getElement().props.disabled).to.be.false;
  });

  //TODO should be returned back after fix version problem
  it.skip('run and save buttons should be disabled', () => {
    const props = {
      ...commonProps,
      currentSql: '12',
      dataset: Immutable.fromJS({
        datasetConfig: {
          version: '22',
          sql: '12',
          fullPathList: ['newSpace', 'newTable']
        }
      })
    };
    wrapper = shallow(<ExploreInfoHeader {...props}/>, {context});
    expect(wrapper.find('.run-button').length).to.equal(1);
    expect(wrapper.find('.run-button').getElement().props.disabled).to.equal(true);
    expect(wrapper.find('.explore-save-button').getElement().props.disabled).to.equal(true);
  });

  //TODO should be returned back after fix version problem
  it.skip('save button should be enabled and run button should be disabled', () => {
    const props = {
      ...commonProps,
      currentSql: '12',
      dataset: Immutable.fromJS({
        datasetConfig: {
          version: '223',
          sql: '12',
          fullPathList: ['newSpace', 'newTable']
        }
      })
    };

    wrapper = shallow(<ExploreInfoHeader {...props}/>, {context});
    expect(wrapper.find('.run-button').length).to.equal(1);
    expect(wrapper.find('.run-button').getElement().props.disabled).to.equal(true);
    expect(wrapper.find('.explore-save-button').getElement().props.disabled).to.equal(false);
  });

  describe('#isTransformNeeded', () => {
    it('should return true if sql has changed', () => {
      wrapper.setProps({currentSql: 'different sql'});
      expect(instance.isTransformNeeded()).to.be.true;
    });

    it('should return true if queryContext has changed', () => {
      wrapper.setProps({queryContext: Immutable.List(['different sql'])});
      expect(instance.isTransformNeeded()).to.be.true;
    });

    it('should return true if dataset has no version', () => {
      wrapper.setProps({dataset: commonProps.dataset.remove('datasetVersion')});
      expect(instance.isTransformNeeded()).to.be.true;
    });

    it('should return false if none of the above are true', () => {
      const { dataset } = commonProps;
      wrapper.setProps({currentSql: dataset.get('sql'), queryContext: dataset.get('context')});
      expect(instance.isTransformNeeded()).to.be.false;

      wrapper.setProps({currentSql: null});
      expect(instance.isTransformNeeded()).to.be.false;
    });
  });

  describe('#handlePreviewClick', () => {
    it('should call previewDatasetSql', () => {
      sinon.stub(instance, 'navigateToExploreTableIfNecessary');
      instance.handlePreviewClick();
      expect(commonProps.previewDatasetSql).to.have.been.called;
    });
  });

  describe('#handleRunClick', () => {
    it('should call runDatasetSql', () => {
      sinon.stub(instance, 'navigateToExploreTableIfNecessary');
      instance.handleRunClick();
      expect(commonProps.runDatasetSql).to.have.been.called;
    });
  });

  describe('#navigateToExploreTableIfNecessary', () => {
    it('should navigate to url parent path only if props.pageType !== default', () => {
      instance.navigateToExploreTableIfNecessary();
      expect(context.router.push).to.not.be.called;

      wrapper.setProps({location: {pathname: '/home/space/ds1/graph'}, pageType: 'graph'});
      instance.navigateToExploreTableIfNecessary();
      expect(context.router.push).to.be.calledWith({pathname: '/home/space/ds1'});
    });
  });

  describe('Saving and BI', () => {
    describe('handleSave method', () => {
      it('should transformIfNecessary, then transformHistoryCheck, then save with nextAction', () => {
        instance.setState({nextAction: 'nextAction'});

        sinon.stub(instance, 'transformIfNecessary').callsFake((callback) => callback('foo'));
        instance.handleSave();
        expect(commonProps.transformHistoryCheck).to.be.called;
        commonProps.transformHistoryCheck.args[0][1]();
        expect(commonProps.saveDataset).to.be.called;
      });
    });

    describe('handleShowBI', () => {
      it('should call saveAsDataset displayFullPath[0] is tmp', () => {
        wrapper.setProps({dataset: commonProps.dataset.setIn(['displayFullPath', 0], 'tmp')});
        sinon.stub(instance, 'transformIfNecessary').callsFake((callback) => callback('foo'));
        instance.handleShowBI(NEXT_ACTIONS.openTableau);
        return expect(commonProps.saveAsDataset.called).to.be.true;
      });
    });

    describe('isEditedDataset', () => {
      it('should return false if dataset.datasetType is missing', () => {
        wrapper.setProps({dataset: commonProps.dataset.set('datasetType', undefined)});
        expect(instance.isEditedDataset()).to.be.false;
      });

      it('should return false if datasetType starts with PHYSICAL_DATASET', () => {
        wrapper.setProps({
          dataset: Immutable.fromJS({
            datasetType: 'PHYSICAL_DATASET'
          })
        });
        expect(instance.isEditedDataset()).to.be.false;
      });

      it('should always return false for "New Query"', () => {
        const dataset = commonProps.dataset.setIn(['displayFullPath', 0], 'tmp');
        wrapper.setProps({dataset});
        expect(instance.isEditedDataset()).to.be.false;
        // verify difference between tipVersion and initialDatasetVersion
        wrapper.setProps({ currentSql: commonProps.dataset.get('sql'), initialDatasetVersion: '1' });
        expect(instance.isEditedDataset()).to.be.false;
      });

      it('should return true if dataset sql is different from currentSql', () => {
        wrapper.setProps({currentSql: 'different sql'});
        expect(instance.isEditedDataset()).to.be.true;
      });

      it('should return history.isEdited if none of the above are true', () => {
        wrapper.setProps({currentSql: commonProps.dataset.get('sql'), history: undefined});
        expect(instance.isEditedDataset()).to.be.false;
        wrapper.setProps({currentSql: null});
        expect(instance.isEditedDataset()).to.be.false;
        wrapper.setProps({history: Immutable.Map({isEdited: false})});
        expect(instance.isEditedDataset()).to.be.false;
        wrapper.setProps({history: Immutable.Map({isEdited: true})});
        expect(instance.isEditedDataset()).to.be.true;
      });
    });

    describe('#transformIfNecessary', () => {
      it('should call transformHistoryCheck only if needsTranform', () => {
        sinon.stub(instance, 'isTransformNeeded').returns(false);
        instance.transformIfNecessary();
        expect(commonProps.transformHistoryCheck).to.not.be.called;

        instance.isTransformNeeded.returns(true);
        instance.transformIfNecessary();
        expect(commonProps.transformHistoryCheck).to.be.called;
      });

      it('should call navigateToExploreTableIfNecessary if performing transform', () => {
        sinon.stub(instance, 'isTransformNeeded').returns(true);
        sinon.spy(instance, 'navigateToExploreTableIfNecessary');
        instance.transformIfNecessary(() => {});
        expect(instance.navigateToExploreTableIfNecessary).to.be.called;
      });
    });

  });

  describe('#isCreatedAndNamedDataset', function() {
    it('should return false when dataset is new ', function() {
      wrapper.setProps({dataset: Immutable.Map({isNewQuery: true})});
      expect(instance.isCreatedAndNamedDataset()).to.be.false;
    });

    it('should return false when dataset have not been saved under a name ', function() {
      wrapper.setProps({
        dataset: Immutable.fromJS({isNewQuery: false, datasetVersion: '1234', displayFullPath: ['tmp']})
      });
      expect(instance.isCreatedAndNamedDataset()).to.be.false;
    });

    it('should return true when dataset has been saved under a name ', function() {
      wrapper.setProps({
        dataset: Immutable.fromJS({isNewQuery: false, datasetVersion: '1234', displayFullPath: ['ds']})
      });
      expect(instance.isCreatedAndNamedDataset()).to.be.true;
    });
  });

  describe('#shouldEnableSettingsButton', function() {
    it('should return false when isCreatedAndNamedDataset returns false', function() {
      sinon.stub(instance, 'isCreatedAndNamedDataset').returns(false);
      expect(instance.shouldEnableSettingsButton(Immutable.Map())).to.be.false;
    });

    it('should return false when dataset is edited', function() {
      sinon.stub(instance, 'isCreatedAndNamedDataset').returns(false);
      sinon.stub(instance, 'isEditedDataset').returns(true);
      expect(instance.shouldEnableSettingsButton(Immutable.Map())).to.be.false;
    });

    it('should return true when neither of the above occur ', function() {
      sinon.stub(instance, 'isCreatedAndNamedDataset').returns(true);
      sinon.stub(instance, 'isEditedDataset').returns(false);
      expect(instance.shouldEnableSettingsButton(Immutable.Map())).to.be.true;
    });

  });

  describe('#renderLeftPartOfHeader', () => {

    it('should render an empty div if !dataset.datasetType', () => {
      const node = shallow(instance.renderLeftPartOfHeader(commonProps.dataset.delete('datasetType'), false));
      expect(node.find('div')).to.have.length(1);
    });

    it('should render dataset label when dataset has datasetType present', () => {
      const node = shallow(instance.renderLeftPartOfHeader(commonProps.dataset, false));
      expect(node.find('.title-wrap').children()).to.have.length(1);
    });
  });

  describe('#renderDatasetLabel', () => {

    it('should render (edited) dataset label when isEditedDataset returns true', () => {
      sinon.stub(instance, 'isEditedDataset').returns(true);
      const node = shallow(instance.renderDatasetLabel(commonProps.dataset));
      expect(node.find('FontIcon')).to.have.length(1);
      expect(node.find('EllipsedText').props().text).to.be.contains('displayTable{"0":{"id":"Dataset.Edited"}}');
    });

    it('should not render (edited) dataset label when isEditedDataset returns false', () => {
      sinon.stub(instance, 'isEditedDataset').returns(false);
      const node = shallow(instance.renderDatasetLabel(commonProps.dataset));
      expect(node.find('FontIcon')).to.have.length(1);
      expect(node.find('EllipsedText').props().text).to.be.contains('displayTable');
      expect(node.find('EllipsedText').props().text).to.be.not.contains('(edited)');
    });
  });

  describe('right part rendering', () => {
    it('should render settings button in enabled state when dataset is not new and saved', () => {
      wrapper.setProps({
        initialDatasetVersion: '1',
        currentSql: '23',
        dataset: commonProps.dataset.set('isNewQuery', false).set('tipVersion', '1')
      });
      const settingsButton = wrapper.find('ExploreSettingsButton');
      expect(settingsButton.props().disabled).to.be.false;
    });
  });
});
