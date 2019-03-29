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

import { ViewStateWrapper, findFirstTruthyValue, mergeViewStates } from './ViewStateWrapper';

describe('ViewStateWrapper', () => {

  let viewState;
  beforeEach(() => {
    viewState = Immutable.Map({isInProgress: false, isFailed: false});
  });

  describe('when nothing is happening', () => {
    it('should render children', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}>
        <button/>
      </ViewStateWrapper>);
      expect(wrapper.find('button')).to.have.length(1);
      expect(wrapper.find('Spinner')).to.have.length(0);
    });
  });

  describe('when isInProgress', () => {
    beforeEach(() => {
      viewState = viewState.set('isInProgress', true);
    });

    // overlay prevents interaction with children
    it('should render children and overlay if hideChildrenWhenInProgress=false', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}><button/></ViewStateWrapper>);
      expect(wrapper.find('.view-state-wrapper-overlay')).to.have.length(1);
      expect(wrapper.children().find('button')).to.have.length(1);

      wrapper.setProps({hideChildrenWhenInProgress: true});
      expect(wrapper.children().find('button')).to.have.length(0);
      expect(wrapper.find('.view-state-wrapper-overlay')).to.have.length(1);
    });

    it('should render spinner if state.shouldWeSeeSpinner=true', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}/>);
      expect(wrapper.find('Spinner')).to.have.length(0);
      wrapper.setState({shouldWeSeeSpinner: true});
      expect(wrapper.find('Spinner')).to.have.length(1);
    });

    it('should set shouldWeSeeSpinner after spinnerDelay', (done) => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState} spinnerDelay={0}/>);
      expect(wrapper.state('shouldWeSeeSpinner')).to.be.false;
      setTimeout(() => {
        expect(wrapper.state('shouldWeSeeSpinner')).to.be.true;
        done();
      }, 10);
    });

    it('should not show spinner when isInProgress=false at end of spinnerDelay', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState} spinnerDelay={0}/>);
      expect(wrapper.state('shouldWeSeeSpinner')).to.be.false;
      wrapper.setProps({viewState: viewState.set('isInProgress', false)});
      setTimeout(() => {
        expect(wrapper.state('shouldWeSeeSpinner')).to.be.false;
      }, 10);
    });

    it('should unset shouldWeSeeSpinner when isInProgress becomes false', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}/>);
      wrapper.setState({shouldWeSeeSpinner: true});
      wrapper.setProps({viewState: viewState.set('isInProgress', false)});
      expect(wrapper.state('shouldWeSeeSpinner')).to.be.false;
    });
  });

  describe('when isFailed', () => {
    beforeEach(() => {
      viewState = new Immutable.fromJS({
        isInProgress: false,
        isFailed: true,
        error: {
          message: 'message',
          dismissed: false,
          id: 'messageId'
        }
      });
    });

    it('should render error message', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}/>);
      const messageProps = wrapper.find('Message').first().props();
      expect(messageProps.message).to.equal(viewState.getIn(['error', 'message']));
      expect(messageProps.dismissed).to.equal(viewState.getIn(['error', 'dismissed']));
      expect(messageProps.messageId).to.equal(viewState.getIn(['error', 'id']));
      expect(messageProps.messageType).to.equal('error');
    });
  });

  describe('when isWarning', () => {
    beforeEach(() => {
      viewState = new Immutable.fromJS({
        isInProgress: false,
        isFailed: false,
        isWarning: true,
        error: {
          message: 'message',
          dismissed: false,
          id: 'messageId'
        }
      });
    });

    it('should render warning message', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState}/>);
      const messageProps = wrapper.find('Message').first().props();
      expect(messageProps.message).to.equal(viewState.getIn(['error', 'message']));
      expect(messageProps.dismissed).to.equal(viewState.getIn(['error', 'dismissed']));
      expect(messageProps.messageId).to.equal(viewState.getIn(['error', 'id']));
      expect(messageProps.messageType).to.equal('warning');
    });
  });

  describe('#renderChildren', () => {
    it('should render children when isInProgress only if hideChildrenWhenInProgress=true', () => {
      const wrapper = shallow(
        <ViewStateWrapper viewState={viewState.set('isInProgress', true)}><div/></ViewStateWrapper>
      );
      expect(wrapper.instance().renderChildren()).to.not.be.undefined;

      wrapper.setProps({hideChildrenWhenInProgress: true});
      expect(wrapper.instance().renderChildren()).to.be.undefined;
    });

    it('should render children when isFailed only if hideChildrenWhenFailed=true', () => {
      const wrapper = shallow(<ViewStateWrapper viewState={viewState.set('isFailed', true)}><div/></ViewStateWrapper>);
      expect(wrapper.instance().renderChildren()).to.be.undefined;

      wrapper.setProps({hideChildrenWhenFailed: false});
      expect(wrapper.instance().renderChildren()).to.not.be.undefined;
    });
  });

  it('renders Message when isFailed unless showMessage=false', () => {
    const message = 'failed';
    const failedState = viewState.set('isFailed', true).setIn(['error', 'message'], message);
    const wrapper = shallow(<ViewStateWrapper viewState={failedState}>
      <button/>
    </ViewStateWrapper>);
    expect(wrapper.find('button')).to.have.length(0);
    expect(wrapper.find('Message')).to.have.length(1);
    expect(wrapper.find('Message').first().props().message).to.eql(message);

    const wrapper2 = shallow(<ViewStateWrapper viewState={failedState} showMessage={false}>
      <button/>
    </ViewStateWrapper>);
    expect(wrapper2.find('Message')).to.have.length(0);
  });
});

describe('findFirstTruthyValue', () => {
  const fieldName = 'testField';
  const generateList = (...list) => list.map(v => (Immutable.fromJS({ [fieldName]: v })));

  it('returns undefined if map list is empty', () => {
    expect(findFirstTruthyValue(fieldName)).to.be.undefined;
  });

  it('returns a only truthy value', () => {
    expect(findFirstTruthyValue(fieldName, ...generateList(0, '', 'testValue', false))).to.be.equal('testValue');
  });

  it('returns a first truthy value', () => {
    expect(findFirstTruthyValue(fieldName, ...generateList(0, '', true, 'testValue', false))).to.be.true;
  });
});

it('mergeViewStates merges view state correctly', () => {
  const result = mergeViewStates(
    Immutable.fromJS({
      isInProgress: false,
      isFailed: true,
      isWarning: false,
      error: 'test error message'
    }),
    Immutable.fromJS({
      isInProgress: true,
      isFailed: false,
      isWarning: true,
      error: null
    })
  );

  expect(result.toJS()).to.be.eql({
    isInProgress: true,
    isFailed: true,
    isWarning: true,
    error: 'test error message'
  });
});
