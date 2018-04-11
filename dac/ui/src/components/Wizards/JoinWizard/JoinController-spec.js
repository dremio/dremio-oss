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
import { minimalFormProps } from 'testUtil';

import CancelablePromise from 'utils/CancelablePromise';
import { RECOMMENDED_JOIN, CUSTOM_JOIN} from 'constants/explorePage/joinTabs';
import StepWizard from '../components/StepWizard';
import { JoinController } from './JoinController';

describe('JoinController', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  let contextTypes;
  beforeEach(() => {
    minimalProps = {
      ...minimalFormProps(),
      dataset: Immutable.fromJS({
        displayFullPath: ['foo']
      }),
      fields: { activeDataset: { value: [] } },
      submit: sinon.stub().returns(Promise.resolve()),
      cancel: sinon.spy(),
      loadRecommendedJoin: sinon.spy(),
      viewState: Immutable.Map({isInProgress: false}),
      location: { query: {} },
      resetJoins: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      handleSubmit: sinon.spy(),
      dataset: Immutable.fromJS({
        apiLinks: {self: ''},
        displayFullPath: ['foo']
      }),
      changeFormType: sinon.spy(),
      location: { query: {}, pathname: '', state: {} },
      loadJoinDataset: sinon.spy(),
      setJoinStep: sinon.spy(),
      fields: {
        activeDataset: { value: ['dataset1'], onChange: sinon.spy() },
        columns: { value: ['column1'], onChange: sinon.spy() }
      },
      values: { activeDataset: ['dataset1'] }
    };
    contextTypes = {
      router: { replace: sinon.spy(), push: sinon.spy() }
    };
    wrapper = shallow(<JoinController {...commonProps}/>, {context: contextTypes});
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<JoinController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render <div>, StepWizard', () => {
    expect(wrapper.type()).to.equal('div');
    expect(wrapper.find('.join')).to.have.length(1);
    expect(wrapper.find(StepWizard)).to.have.length(1);
  });

  it('should not render Preview button for recommended join', () => {
    wrapper.setProps({
      joinTab: RECOMMENDED_JOIN
    });
    const wizardFooterProps = wrapper.find('DefaultWizardFooter').props();
    expect(wizardFooterProps.isPreviewAvailable).to.be.false;
  });

  it('should render Preview button for custom join on step 2', () => {
    wrapper.setProps({
      joinTab: CUSTOM_JOIN,
      joinStep: 2
    });
    const wizardFooterProps = wrapper.find('DefaultWizardFooter').props();
    expect(wizardFooterProps.isPreviewAvailable).to.be.true;
  });

  it('should load recommendations', () => {
    instance.tryToLoadRecommendations(commonProps);
    expect(commonProps.loadRecommendedJoin.called).to.eql(true);
  });

  describe('#componentWillUnmount', () => {
    it('should cancel recommendationsPromise if it exists', () => {
      instance.componentWillUnmount();

      instance.recommendationsPromise = {cancel: sinon.spy()};
      instance.componentWillUnmount();
      expect(instance.recommendationsPromise.cancel).to.be.called;
    });
  });

  describe('#loadRecommendations', () => {
    it('should cancel recommendationsPromise if it exists', () => {
      const cancel = sinon.spy();
      instance.recommendationsPromise = {cancel};
      instance.loadRecommendations();
      expect(cancel).to.be.called;
    });

    it('should call props.loadRecommendedJoin with correct href and set recommendationsPromise', () => {
      instance.loadRecommendations();
      const href = commonProps.dataset.getIn(['apiLinks', 'self']) + '/join_recs';
      expect(commonProps.loadRecommendedJoin).to.be.calledWith({href});

      expect(instance.recommendationsPromise).to.be.instanceOf(CancelablePromise);
    });
  });

  describe('#resetPreviewInfo', () => {
    let _instance;
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        joinTab: CUSTOM_JOIN,
        fields: {
          activeDataset: {onChange: sinon.stub()},
          columns: {onChange: sinon.stub()}
        }
      };
      _instance = shallow(<JoinController {...props}/>, {context: contextTypes}).instance();
    });

    it('should reset activeDataset field value and preview version when tab changed', () => {
      const nextProps = {
        ...props,
        joinTab: RECOMMENDED_JOIN
      };
      _instance.resetPreviewInfo(nextProps);
      expect(props.fields.activeDataset.onChange).to.be.calledWith('');
      expect(props.fields.columns.onChange).to.be.calledWith([]);
      expect(contextTypes.router.replace.args[0][0].state.previewVersion).to.equal('');
    });

    it('should not reset activeDataset field value and preview version when tab not changed', () => {
      _instance.resetPreviewInfo(props);
      expect(props.fields.activeDataset.onChange).to.not.be.called;
      expect(contextTypes.router.replace).to.be.not.be.called;
    });

    it('should not reset activeDataset field value and preview version when tab join wizard on step 2', () => {
      const nextProps = {
        ...props,
        joinTab: RECOMMENDED_JOIN,
        joinStep: 2
      };
      _instance.resetPreviewInfo(nextProps);
      expect(props.fields.activeDataset.onChange).to.not.be.called;
      expect(contextTypes.router.replace).to.be.not.be.called;
    });
  });

  describe('#tryToPreviewRecommendedJoin', () => {
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        joinTab: RECOMMENDED_JOIN,
        activeRecommendedJoin: Immutable.fromJS({ id: '1'}),
        recommendedJoins: Immutable.List([1]),
        values: {
          activeDataset: ['ds'],
          columns: [{joinedColumn: 'A', joinedTableKeyColumnName: 'B'}]
        }
      };
    });

    it('should not perform preview when active tab is not RECOMMENDED_JOIN', () => {
      wrapper.setProps({
        joinTab: RECOMMENDED_JOIN
      });
      expect(commonProps.submit).to.not.be.called;
    });

    it('should not perform preview when recommendedJoins are empty', () => {
      wrapper.setProps({
        recommendedJoins: Immutable.List()
      });
      expect(commonProps.submit).to.not.be.called;
    });

    it('should not perform preview when columns field is empty', () => {
      wrapper.setProps({
        joinTab: RECOMMENDED_JOIN,
        activeRecommendedJoin: Immutable.fromJS({ id: '1'}),
        recommendedJoins: Immutable.List([1]),
        values: {
          activeDataset: ['ds'],
          columns: []
        }
      });
      expect(commonProps.submit).to.not.be.called;
    });

    it('should not perform preview when active recommended join is empty', () => {
      wrapper.setProps({
        joinTab: RECOMMENDED_JOIN,
        activeRecommendedJoin: Immutable.fromJS({ id: '1'}),
        recommendedJoins: Immutable.List(),
        values: {
          activeDataset: ['ds'],
          columns: [{joinedColumn: 'A', joinedTableKeyColumnName: 'B'}]
        }
      });
      expect(commonProps.submit).to.not.be.called;
    });

    it('should perform preview when active recommended changed', () => {
      const _wrapper = shallow(<JoinController {...props}/>, {context: contextTypes});
      _wrapper.setProps({
        activeRecommendedJoin: props.activeRecommendedJoin
      });
      expect(props.submit).to.not.be.called;
      _wrapper.setProps({
        activeRecommendedJoin: Immutable.Map({id: 2})
      });
      expect(props.submit).to.be.called;
    });

    it('should perform preview when changed: recommended join, active dataset field, columns are not empty', () => {
      wrapper.setProps(props);
      expect(commonProps.submit).to.be.called;
    });

    it('should set state.previewError if this.submit was called and throw an error', () => {
      const error = {_error: {message: 'error', id: '1'}};
      const nextProps = {
        ...commonProps,
        activeRecommendedJoin: Immutable.fromJS({ id: '2'}),
        values: {
          columns: [{joinedColumn: 'A', joinedTableKeyColumnName: 'B'}]
        },
        recommendedJoins: Immutable.List([2]),
        joinTab: RECOMMENDED_JOIN
      };
      sinon.stub(instance, 'submit').returns({
        catch: c => c(error)
      });
      instance.tryToPreviewRecommendedJoin(nextProps);
      expect(instance.submit).to.be.called;
      expect(wrapper.state('previewError')).to.equal(error);
    });
  });

  it('should select dataset', () => {
    instance.selectDataset();
    expect(commonProps.setJoinStep).to.be.called;
  });

  describe('#tryToLoadJoinDataset', () => {
    it('should not call loadJoinDataset is not on CUSTOM_JOIN tab', () => {
      const props = {joinTab: RECOMMENDED_JOIN, loadJoinDataset: sinon.spy()};
      instance.tryToLoadJoinDataset(props);
      expect(props.loadJoinDataset).to.not.be.called;
    });

    it('should not call loadJoinDataset if activeDataset is undefined', () => {
      const props = {joinTab: CUSTOM_JOIN, values: {}, loadJoinDataset: sinon.spy()};
      instance.tryToLoadJoinDataset(props);
      expect(props.loadJoinDataset).to.not.be.called;
    });

    it('should call loadJoinDataset if activeDataset is set', () => {
      const props = {joinTab: CUSTOM_JOIN, values: {activeDataset: ['path']}, loadJoinDataset: sinon.spy()};
      instance.tryToLoadJoinDataset(props);
      expect(props.loadJoinDataset).to.be.called;
    });
  });

  describe('#renderPreviewErrorMessage', () => {
    it('should render PreviewErrorMessage if we have previewError in state', () => {
      wrapper.setState({ previewError: {_error: {message: 'error', id: '1'}} });
      expect(wrapper.find('Message')).to.have.length(1);
      expect(wrapper.find('Message').props().message).to.be.eql('error');
    });

    it('should not render PreviewErrorMessage if there is no previewError in state', () => {
      expect(wrapper.find('Message')).to.have.length(0);
    });
  });
});
