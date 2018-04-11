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
import datasetSchema from 'schemas/v2/fullDataset';

import {RECOMMENDED_JOINS_VIEW_ID} from 'components/Wizards/DetailsWizard';
import RecommendedJoinItem from './RecommendedJoinItem';

const recommendedJoins = [{
  'joinType': 'INNER',
  'matchingKeys': {
    'age': 'user'
  },
  'rightTableFullPathList': ['Prod-Sample', 'ds1']
}, {
  'joinType': 'FULLOUTER',
  'matchingKeys': {
    'age': 'user'
  },
  rightTableFullPathList: ['Prod-Sample', 'ds2']
}];

import { RecommendedJoins } from './RecommendedJoins';

describe('RecommendedJoins', () => {
  let commonProps;
  let minimalProps;
  let context;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      recommendedJoins: Immutable.fromJS(recommendedJoins),
      setActiveRecommendedJoin: sinon.spy(),
      activeRecommendedJoin: Immutable.fromJS(recommendedJoins).first()
    };
    commonProps = {
      ...minimalProps,
      ...minimalFormProps(['activeDataset', 'joinType', 'columns']),
      loadExploreEntities: sinon.spy(),
      resetActiveRecommendedJoin: sinon.spy(),
      editRecommendedJoin: sinon.spy()
    };
    context = {
      router: { push: sinon.spy() }
    };
    wrapper = shallow(<RecommendedJoins {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<RecommendedJoins {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render', () => {
    expect(wrapper.hasClass('recommended-joins')).to.equal(true);
  });

  it('should render RecommendedJoinItems', () => {
    expect(wrapper.find(RecommendedJoinItem)).to.have.length(recommendedJoins.length);
  });

  it('should handle recommendations=undefined', () => {
    wrapper = shallow(<RecommendedJoins {...commonProps} recommendedJoins={undefined}/>, {context});
    expect(wrapper.find(RecommendedJoinItem)).to.have.length(0);
  });

  describe('#componentDidMount', () => {
    beforeEach(() => {
      sinon.stub(RecommendedJoins.prototype, 'selectJoin');
    });
    afterEach(() => {
      RecommendedJoins.prototype.selectJoin.restore();
    });

    it('should set active recommended join as first item from recommended joins list', () => {
      shallow(<RecommendedJoins {...commonProps}/>, {context}).instance().componentDidMount();
      expect(RecommendedJoins.prototype.selectJoin)
        .to.be.calledWith(commonProps.recommendedJoins.first(), false);
    });
  });

  describe('#componentWillUnmount', () => {
    it('should reset active recommended join when unmounted', () => {
      shallow(<RecommendedJoins {...commonProps}/>, {context}).instance().componentWillUnmount();
      expect(commonProps.resetActiveRecommendedJoin).to.be.called;
    });

    it('should cancel loadTablePromise if it exists', () => {
      instance.componentWillUnmount();

      instance.loadTablePromise = {cancel: sinon.spy()};
      instance.componentWillUnmount();
      expect(instance.loadTablePromise.cancel).to.be.called;
    });
  });

  describe('#loadRecommendedTable', () => {
    const recommendation = Immutable.fromJS({
      rightTableFullPathList: ['a', 'b'],
      links: {
        data: 'dataLink'
      }
    });
    it('should cancel loadTablePromise if it exists', () => {
      const cancel = sinon.spy();
      instance.loadTablePromise = {cancel};
      instance.loadRecommendedTable(recommendation);
      expect(cancel).to.be.called;
    });

    it('should call props.loadTablePromise with correct href and set loadTablePromise', () => {
      sinon.stub(instance, 'getUniqRecommendationId').returns('someId');
      instance.loadRecommendedTable(recommendation);
      expect(commonProps.loadExploreEntities).to.be.calledWith({
        href: recommendation.getIn(['links', 'data']),
        schema: datasetSchema,
        viewId: RECOMMENDED_JOINS_VIEW_ID,
        uiPropsForEntity: [{key: 'version', value: 'someId'}]
      });

      expect(instance.loadTablePromise).to.be.instanceOf(CancelablePromise);
    });
  });

  describe('#selectJoin', () => {
    beforeEach(() => {
      sinon.stub(instance, 'updateLocation');
      sinon.stub(instance, 'loadRecommendedTable').returns(Promise.resolve());
      sinon.stub(instance, 'updateFormFields');
    });

    it('should update recommended join and form fields when recommendation is not active', () => {
      const activeRecommendation = commonProps.recommendedJoins.get(1);
      instance.selectJoin(activeRecommendation, false);
      expect(instance.updateFormFields).to.be.calledWith(activeRecommendation);
      expect(instance.props.setActiveRecommendedJoin).to.be.calledWith(activeRecommendation);
      expect(instance.loadRecommendedTable).to.not.be.called;
    });

    it('should not update recommended join when item is already selected', () => {
      const selectedRecommendation = commonProps.recommendedJoins.first();
      instance.selectJoin(selectedRecommendation, false);
      expect(instance.updateFormFields).to.not.be.called;
      expect(instance.loadRecommendedTable).to.not.be.called;
      expect(instance.props.setActiveRecommendedJoin).to.not.be.called;
    });

    it('should call loadRecommendedTable and editRecommendedJoin if isCustom is true', () => {
      const activeRecommendation = commonProps.recommendedJoins.get(1);
      instance.selectJoin(activeRecommendation, true);
      expect(instance.updateFormFields).to.be.calledWith(activeRecommendation);
      expect(instance.loadRecommendedTable).to.be.calledWith(activeRecommendation);
      expect(instance.props.setActiveRecommendedJoin).to.not.be.called;
    });
  });

  describe('#updateFormFields', () => {
    it('should trigger the appropriate onChange calls', () => {
      const activeRecommendation = commonProps.recommendedJoins.first();
      instance.updateFormFields(activeRecommendation);

      expect(commonProps.fields.activeDataset.onChange).to.be.calledWith(['Prod-Sample', 'ds1']);
      expect(commonProps.fields.columns.onChange)
        .to.be.calledWith([{ joinedColumn: 'age', joinedTableKeyColumnName: 'user' }]);
      expect(commonProps.fields.joinType.onChange).to.be.calledWith('INNER');
    });
  });
});
