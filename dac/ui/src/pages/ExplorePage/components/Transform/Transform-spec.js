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
import { TEXT, MAP, LIST } from 'constants/DataTypes';
import TransformView from 'pages/ExplorePage/components/Transform/TransformView';

import { Transform } from './Transform';

describe('Transform', () => {
  let minimalProps;
  let context;
  let wrapper;
  let instance;
  beforeEach(() => {
    context = {
      router: {
        push: sinon.spy(),
        replace: sinon.spy()
      }
    };
    minimalProps = {
      transform: Immutable.Map({ columnType: 'TEXT' }),
      viewState: Immutable.Map(),
      cards: Immutable.List(),
      cardsViewState: Immutable.Map(),
      submit: sinon.spy(),
      loadTransformCards: sinon.spy(),
      loadTransformCardPreview: sinon.spy(),
      loadTransformValuesPreview: sinon.spy(),
      resetViewState: sinon.spy(),
      location: {},
      changeFormType: () => {}
    };
    wrapper = shallow(<Transform {...minimalProps}/>, { context });
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<Transform {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render TransformView', () => {
    expect(wrapper.find(TransformView).length).to.eql(1);
  });

  describe('componentDidMount', () => {
    let action;
    beforeEach(() => {
      sinon.stub(instance, 'loadTransformCards').callsFake(() => Promise.resolve(action));
    });
    it('should loadTransformCards and resetViewState', () => {
      instance.componentDidMount();
      expect(minimalProps.resetViewState).to.be.calledOnce;
      expect(instance.loadTransformCards).to.be.calledOnce;
    });
    it('should call router.replace if method is Values and all availableValues are unique', () => {
      action = { payload: {values: {availableValues: [{count: 1}]}}};
      const props = {
        ...minimalProps,
        transform: Immutable.fromJS({
          method: 'Values',
          columnType: 'TEXT'
        })
      };
      wrapper.setProps(props);
      instance.componentDidMount();
      expect(instance.loadTransformCards).to.be.called;
      return instance.loadTransformCards().then(() => {
        expect(context.router.replace).to.be.calledOnce;
        expect(context.router.replace).to.calledWith({
          ...minimalProps.location,
          state: {
            ...minimalProps.location.state,
            method: 'Pattern'
          }
        });
      });
    });
    it('should not call router.replace if action is undefined', () => {
      action = undefined;
      const props = {
        ...minimalProps,
        transform: Immutable.fromJS({
          method: 'Values',
          columnType: 'TEXT'
        })
      };
      wrapper.setProps(props);
      instance.componentDidMount();
      expect(instance.loadTransformCards).to.be.called;
      return instance.loadTransformCards().then(() => {
        expect(context.router.replace).to.not.been.called;
      });
    });
    it('should call router.replace if availableValues is empty', () => {
      action = { payload: {values: {availableValues: []}}};
      const props = {
        ...minimalProps,
        transform: Immutable.fromJS({
          method: 'Values',
          columnType: 'TEXT'
        })
      };
      wrapper.setProps(props);
      instance.componentDidMount();
      expect(instance.loadTransformCards).to.be.called;
      return instance.loadTransformCards().then(() => {
        expect(context.router.replace).to.be.calledOnce;
        expect(context.router.replace).to.calledWith({
          ...minimalProps.location,
          state: {
            ...minimalProps.location.state,
            method: 'Pattern'
          }
        });
      });
    });
    it('should not call router.replace if transform.method is not Values', () => {
      action = { payload: {values: {availableValues: []}}};
      const props = {
        ...minimalProps,
        transform: Immutable.fromJS({
          method: 'Pattern',
          columnType: 'TEXT'
        })
      };
      wrapper.setProps(props);
      instance.componentDidMount();
      expect(instance.loadTransformCards).to.be.called;
      return instance.loadTransformCards().then(() => {
        expect(context.router.replace).to.not.been.called;
      });
    });
  });

  describe('componentWillReceiveProps', () => {
    const transform = Immutable.fromJS({transformType: 'type1', columnType: 'cType1'});
    beforeEach(() => {
      wrapper.setProps({transform});
      sinon.stub(instance, 'loadTransformCards');
    });
    it('should loadTransformCards when transform property was changed ', () => {
      instance.componentWillReceiveProps({transform: {transformType: 'type2', columnType: 'cType2'}});
      expect(instance.loadTransformCards).to.be.calledOnce;
    });
    it('shouldn\'t loadTransformCards when transform property wasn\'t changed ', () => {
      instance.componentWillReceiveProps({transform});
      expect(instance.loadTransformCards).to.not.be.called;
    });
  });

  describe('loadTransformCards', () => {
    it('should do nothing when cellText or mapPathList are empty (no selection)', () => {
      const transform = Immutable.fromJS({selection: {cellText: null, mapPathList: null}, columnType: TEXT});
      wrapper.setProps({transform});
      instance.loadTransformCards(instance.props);
      expect(instance.props.loadTransformCards).to.not.be.called;
    });

    it('should do nothing columnType is LIST or MAP and transformType = extract', () => {
      const transform = Immutable.fromJS({
        transformType: 'extract', selection: {cellText: null, mapPathList: ['a', 'b']}, columnType: MAP
      });
      wrapper.setProps({transform});
      instance.loadTransformCards(instance.props);
      expect(instance.props.loadTransformCards).to.not.be.called;

      wrapper.setProps({transform: transform.set('columnType', LIST)});
      instance.loadTransformCards(instance.props);
      expect(instance.props.loadTransformCards).to.not.be.called;

      wrapper.setProps({transform: transform.set('columnType', TEXT)});
      instance.loadTransformCards(instance.props);
      expect(instance.props.loadTransformCards).to.be.called;
    });

    it('should load TransformCards', () => {
      const transform = Immutable.fromJS({
        method: 'Pattern', selection: {cellText: 'sometext', mapPathList: null}, columnType: TEXT});
      wrapper.setProps({transform});
      instance.props.loadTransformCards.reset(); // called in componentDidMount

      sinon.stub(instance, 'transformTypeURLMapper');
      instance.loadTransformCards(instance.props);
      expect(instance.props.loadTransformCards).to.be.calledOnce;
    });
  });

  describe('loadTransformCardPreview', () => {
    it('should run loadTransformCardPreview', () => {
      const transform = Immutable.fromJS({selection: {cellText: 'sometext'}, columnType: TEXT});
      const dataset = Immutable.Map({values: 'sometext'});
      wrapper.setProps({transform, dataset});
      sinon.stub(instance, 'transformTypeURLMapper').returns('actionType');
      instance.loadTransformCardPreview('index', 'model');
      expect(instance.props.loadTransformCardPreview.calledWith({
        selection: 'sometext',
        rule: 'model'
      }, transform, 'dataset', 'index'));
    });
  });

  describe('loadTransformValuesPreview', () => {
    it('should run loadTransformValuesPreview', () => {
      const transform = Immutable.fromJS({
        selection: {
          cellText: 'sometext',
          transformType: 'transformType'
        },
        columnType: TEXT
      });
      const dataset = Immutable.Map({values: 'sometext'});
      wrapper.setProps({transform, dataset});
      instance.loadTransformValuesPreview('values');
      expect(instance.props.loadTransformCardPreview.calledWith({
        selection: 'sometext',
        rule: 'values'
      }, transform, 'dataset', 'index'));
    });
  });

  describe('transformTypeURLMapper', () => {
    it('should return transformType if transformType is "split"', () => {
      expect(instance.transformTypeURLMapper(Immutable.fromJS({transformType: 'split',
        columnType: MAP}))).to.equal('split');
    });
    it('should return transformType when columnType is undefined', () => {
      expect(instance.transformTypeURLMapper(Immutable.fromJS({transformType: 'other type'}))).to.equal('other type');
    });
    it('should return transformType concat "_map" if columnType is MAP', () => {
      expect(instance.transformTypeURLMapper(Immutable.fromJS({transformType: 'type',
        columnType: MAP}))).to.equal('type_map');
    });
    it('should return transformType concat "_list"  if columnType is LIST', () => {
      expect(instance.transformTypeURLMapper(Immutable.fromJS({transformType: 'type',
        columnType: LIST}))).to.equal('type_list');
    });
  });

  describe('handleTransformChange', () => {
    it('should update router', () => {
      wrapper.setProps({location: {'k': 'v'}});
      instance.handleTransformChange(Immutable.fromJS({'path': 'l3'}));
      expect(context.router.push).to.be.calledWith({'k': 'v', state: {'path': 'l3'}});
    });
  });
});
