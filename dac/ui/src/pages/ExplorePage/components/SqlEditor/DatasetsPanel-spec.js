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

import { DatasetsPanel, PARENTS_TAB, BROWSE_TAB, SEARCH_TAB } from './DatasetsPanel';

describe('DatasetsPanel', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    context = {routeParams:{resourceId: '1'}};
    minimalProps = {
      dataset: Immutable.fromJS({}),
      loadParents: sinon.stub().returns(Promise.resolve()),
      viewState: Immutable.fromJS({
        isInProgress: false
      }),
      parentListViewState: Immutable.fromJS({
        isInProgress: false
      }),
      isVisible: true
    };
    commonProps = {
      ...minimalProps,
      height: 1,
      search: {},
      dragType: 't1',
      addFuncToSqlEditor: sinon.spy(),
      loadSearchData: sinon.spy(),
      addFullPathToSqlEditor: sinon.spy(),
      parentList: [],
      dataset: Immutable.fromJS({
        datasetVersion: '1',
        fullPath: ['1']
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetsPanel {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  describe('#componentWillMount', () => {
    it('should load parents for dataset if it is not new', () => {
      shallow(<DatasetsPanel {...commonProps}/>, {context});
      expect(commonProps.loadParents).to.be.calledOnce;
    });

    it('should not load parents for new dataset', () => {
      const props = {
        ...commonProps,
        dataset: Immutable.fromJS({
          isNewQuery: true
        })
      };
      shallow(<DatasetsPanel {...props}/>, {context});
      expect(commonProps.loadParents).to.be.not.called;
    });
  });

  describe('#componentWillReceiveProps', () => {
    let wrapper;
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        dataset: Immutable.fromJS({
          datasetVersion: '123'
        })
      };
      sinon.stub(DatasetsPanel.prototype, 'componentWillMount');
      wrapper = shallow(<DatasetsPanel {...props}/>, {context});
    });
    afterEach(() => {
      DatasetsPanel.prototype.componentWillMount.restore();
    });

    it('should load parents when dataset version is changed only if isVisible', () => {
      wrapper.setProps({
        dataset: Immutable.fromJS({
          datasetVersion: '1234'
        })
      });
      expect(props.loadParents).to.be.calledOnce;

      wrapper.setProps({
        isVisible: false,
        dataset: Immutable.fromJS({
          datasetVersion: '5678'
        })
      });
      expect(props.loadParents).to.be.calledOnce;
    });

    it('should load parents when becoming visible only if datasetVersion exists', () => {
      wrapper.setProps({isVisible: false});
      wrapper.setProps({isVisible: true});
      expect(props.loadParents).to.be.calledOnce;

      wrapper.setProps({isVisible: false, dataset: undefined});
      wrapper.setProps({isVisible: true});
      expect(props.loadParents).to.be.calledOnce;
    });

    it('should not load parents when dataset version not changed', () => {
      wrapper.setProps(props);
      expect(props.loadParents).to.not.be.called;
    });

    it('should not load parents when dataset is new', () => {
      wrapper.setProps({
        dataset: Immutable.fromJS({
          isNewQuery: true
        })
      });
      expect(props.loadParents).to.be.not.called;
    });

    it('should not load parents when dataset is tmp.UNTITLED', () => {
      wrapper.setProps({
        dataset: Immutable.fromJS({
          fullPath: ['tmp', 'UNTITLED']
        })
      });
      expect(props.loadParents).to.be.not.called;
    });

    it('should not load parents when dataset info request is in progress', () => {
      wrapper.setProps({
        dataset: Immutable.fromJS({
          datasetVersion: '1234',
          isNew: false
        }),
        viewState: Immutable.fromJS({
          isInProgress: true
        })
      });
      expect(props.loadParents).to.not.be.called;
    });
  });

  describe('#getActiveTabId', () => {
    it('should return state.activeTabId if defined', () => {
      const wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
      wrapper.setState({activeTabId: SEARCH_TAB});
      expect(wrapper.instance().getActiveTabId()).to.equal(SEARCH_TAB);
    });
    it('should return BROWSE_TAB or PARENTS_TAB if activeTabId is not defined based on shouldShowParentTab', () => {
      const wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
      const instance = wrapper.instance();
      sinon.stub(instance, 'shouldShowParentTab').returns(true);
      expect(instance.getActiveTabId()).to.equal(PARENTS_TAB);
      instance.shouldShowParentTab.returns(false);
      expect(instance.getActiveTabId()).to.equal(BROWSE_TAB);
    });
  });

  describe('#updateActiveTab', () => {
    it('should set state.activeTabId', () => {
      const wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
      wrapper.instance().updateActiveTab(SEARCH_TAB);
      expect(wrapper.state().activeTabId).to.equal(SEARCH_TAB);
    });
  });

  describe('#shouldShowParentTab', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
      instance = wrapper.instance();
    });

    it('should return true if dataset has parents', () => {
      wrapper.setProps({parentList: [{}]});
      expect(instance.shouldShowParentTab()).to.be.true;
    });

    it('should return false if dataset has no parents and not loading', () => {
      expect(instance.shouldShowParentTab()).to.be.false;
    });

    it('should return true if viewState is in progress', () => {
      wrapper.setProps({viewState: commonProps.viewState.set('isInProgress', true)});
      expect(instance.shouldShowParentTab()).to.be.true;
      wrapper.setProps({viewState: commonProps.viewState.set('isInProgress', true)});
    });

    it('should return true if parentListViewState is in progress', () => {
      wrapper.setProps({parentListViewState: commonProps.parentListViewState.set('isInProgress', true)});
      expect(instance.shouldShowParentTab()).to.be.true;
    });

    it('should return false if isNewQuery', () => {
      wrapper.setProps({
        parentList: [{}],
        dataset: commonProps.dataset.set('isNewQuery', true)
      });
      expect(instance.shouldShowParentTab()).to.be.false;
    });
  });

  describe('#renderHeaderTabsItems', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<DatasetsPanel {...commonProps}/>, {context});
      instance = wrapper.instance();
    });

    it('should render all tab items only if shouldShowParentTab returns true', () => {
      let tabs = instance.renderHeaderTabsItems();
      expect(tabs).to.have.length(3);
      expect(tabs[0]).to.be.null;

      sinon.stub(instance, 'shouldShowParentTab').returns(true);
      tabs = instance.renderHeaderTabsItems();
      expect(tabs).to.have.length(3);
      expect(tabs[0]).to.not.be.null;
    });

    it('should hide parents tab when !shouldShowParentTab', () => {
      sinon.stub(instance, 'shouldShowParentTab').returns(false);
      wrapper.setState({
        activeTabId: BROWSE_TAB
      });
      const tabs = instance.renderHeaderTabsItems();
      expect(wrapper.state().activeTabId).to.be.equal(BROWSE_TAB);
      expect(tabs).to.have.length(3);
      expect(tabs[0]).to.be.null;
    });
  });
});
