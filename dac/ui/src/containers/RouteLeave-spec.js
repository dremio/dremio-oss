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
import { shallow, mount } from 'enzyme';
import PropTypes from 'prop-types';
import { HookProviderView, HookConsumer, singleArgFnGenerator } from './RouteLeave';

describe('RouteLeave.js', () => {

  const providerCommonProps = {
    route: {
      path: '/new_query'
    },
    router: {
      setRouteLeaveHook: () => {},
      push: () => {}
    },
    showConfirmationDialog: () => {}
  };

  describe('HookProviderView', () => {
    let commonProps;

    beforeEach(() => {
      commonProps = providerCommonProps;
    });

    it('should call router.setRouteLeaveHook only if route has changed', () => {
      const newProps = {
        ...commonProps,
        router: {
          setRouteLeaveHook: sinon.spy()
        }
      };
      const instance = shallow(<HookProviderView {...newProps} />).instance();
      instance.componentWillReceiveProps(newProps);
      expect(newProps.router.setRouteLeaveHook).to.not.be.called;
      const props = {...newProps, route: { path: '/:resources' }};
      instance.componentWillReceiveProps(props);
      expect(props.router.setRouteLeaveHook).to.be.called;
    });

    it('routeWillLeave returns true, if leave was confirmed', () => {
      const confirmFn = sinon.spy();
      const instance = shallow(<HookProviderView
        {...commonProps}
        showConfirmationDialog={confirmFn} />).instance();
      const hasChangesCallback = () => true; // simulate changes
      instance.addCallback('test id', hasChangesCallback);

      expect(instance.routeWillLeave()).to.eql(false); // should not allow to leave a page;
      expect(confirmFn).to.be.called; // confimation should be called

      confirmFn.reset();
      instance.leaveConfirmed(); // confirm leave;

      expect(instance.routeWillLeave()).to.eql(true); // should allow to leave a page, as there was a confirmation
      expect(confirmFn).to.be.not.called; // confimation should not be called
    });

    describe('showConfirmationDialog', () => {
      let confirmFn;
      let instance;

      beforeEach(() => {
        confirmFn = sinon.spy();
        instance = shallow(<HookProviderView
          {...commonProps}
          showConfirmationDialog={confirmFn} />).instance();
      });

      it('is not called, when there is no changes', () => {
        const hasChangesCallback = () => false; // no changes

        instance.addCallback('test id', hasChangesCallback);
        instance.routeWillLeave(); // simulate route change

        expect(confirmFn).to.be.not.called; // confirm function should NOT be called
      });

      it('reacts on hasChangesCallback result change', () => {
        const hasChangesCallback = sinon.stub();
        instance.addCallback('test id', hasChangesCallback);

        const changeValue = hasChanges => {
          hasChangesCallback.returns(true); // change the result
          instance.routeWillLeave(); // simulate route change
          expect(confirmFn).to.be[hasChanges ? 'called' : 'notCalled']; // should be called or not depending on hasChanges value
        };

        changeValue(true);
        //check transitions
        changeValue(false); // true -> false
        changeValue(true); // false -> true
      });

      it('not called if hasChangesCallback was removed', () => {
        const hasChangesCallback = () => true; // simulate changes

        const removeHandler = instance.addCallback('test id', hasChangesCallback);
        instance.routeWillLeave(); // simulate route change

        expect(confirmFn).to.be.called; // confirm function should be called if there is any change

        removeHandler();

        confirmFn.reset();
        instance.routeWillLeave(); // simulate route change
        expect(confirmFn).to.be.not.called; // should Not be called, as callback is removed and not monitored anymore
      });

      it('is called, when at least one hasChangesCallback returns true', () => {
        const hasChangesCalbackList = Array(5).fill(false);
        const removeHandlers = {};
        const trueIndex = 2;
        hasChangesCalbackList[trueIndex] = true; // third element has changes

        // add all callbacks
        hasChangesCalbackList.forEach((hasChanges, index) => {
          removeHandlers[index] = instance.addCallback(`test id ${index}`, () => hasChanges);
        });
        instance.routeWillLeave(); // simulate route change

        expect(confirmFn).to.be.called;

        removeHandlers[trueIndex](); // remove a handler, that has changes

        confirmFn.reset();
        instance.routeWillLeave(); // simulate route change
        expect(confirmFn).to.be.not.called; // should not be called as the reset of handlers does not have changes
      });

      it('should not be called if redirect reason is unauthorized', () => {
        const hasChangesCallback = sinon.stub();
        instance.addCallback('test id', hasChangesCallback);

        hasChangesCallback.returns(true); // change the result
        instance.routeWillLeave({search: 'abc&reason=401'}); // simulate route change
        expect(confirmFn).to.be.not.called;
      });
    });

  });

  describe('HookConsumer', () => {

    const getRenderFn = (hasChangesCallback) => (addCallback) => {
      addCallback(hasChangesCallback);
      return <div></div>;
    };

    let addCallbackOriginal;
    beforeEach(() => {
      sinon.spy(HookProviderView.prototype, 'addCallback');
      addCallbackOriginal = HookProviderView.prototype.addCallback;
    });

    afterEach(() => {
      addCallbackOriginal.restore();
      addCallbackOriginal = null;
    });

    it('Original Provider.addCallback is called', () => {
      const hasChanges = () => true;
      const renderFn = getRenderFn(hasChanges);

      mount(<HookProviderView {...providerCommonProps}>
        <HookConsumer>
          {renderFn}
        </HookConsumer>
      </HookProviderView>);

      expect(addCallbackOriginal).to.be.called;
      const [id, argFn] = addCallbackOriginal.lastCall.args;
      expect(id).to.exist; // not empty id should be generated
      expect(argFn).to.eql(hasChanges); // addCallback should be called with provided hasChanges function
    });

    it('Unique ids are generated for different consumers ', () => {
      mount(<HookProviderView {...providerCommonProps}>
        <div>
          <HookConsumer>
            {getRenderFn()}
          </HookConsumer>
          <HookConsumer>
            {getRenderFn()}
          </HookConsumer>
        </div>
      </HookProviderView>);

      expect(addCallbackOriginal).to.be.calledTwice; // should be called 2 times, as there are 2 consumers
      const id1 = addCallbackOriginal.args[0][0];
      const id2 = addCallbackOriginal.args[1][0];
      expect(id1).not.eql(id2); // uninque ids should be generated by each consumer
    });

    it('hasChanges callback is removed on unmount', () => {
      const hasChanges = () => true;
      let provider;
      const providerRef = (p) => {
        provider = p;
      };
      const TestComp = ({ mounted, refFn }) => {
        return <HookProviderView {...providerCommonProps} ref={refFn}>
          {
            mounted &&
            <HookConsumer>
              {getRenderFn(hasChanges)}
            </HookConsumer>
          }
        </HookProviderView>;
      };
      TestComp.propTypes = {
        mounted: PropTypes.bool,
        refFn: PropTypes.func
      };
      const wrapper = mount(<TestComp mounted refFn={providerRef} />);
      expect(provider.hasChangeCallbacks).to.have.property('size', 1); // one callback should be added

      wrapper.setProps({ mounted: false }); // unmount consumer
      expect(provider.hasChangeCallbacks).is.empty; // callback should be removed from the list
    });
  });

  // these test are very important from performance point of view.
  // singleArgFnGenerator should return the same result if parameters are not changed, to avoid unnecessary rerenders.
  describe('singleArgFnGenerator', () => {
    let selector;
    const id = 'test id';
    beforeEach(() => {
      selector = singleArgFnGenerator(id, () => {});
    });

    it('wraps a fn correctly', () => {
      const callback1 = sinon.spy();
      const state = {
        fn: callback1
      };
      const hasChangesCallback = () => {};

      const resultFn = selector(state);
      resultFn(hasChangesCallback);

      expect(callback1).to.be.calledWith(id, hasChangesCallback); // result function should add id as first argument to original function
    });

    it('result function should return the same function, if input values are the same', () => {
      const fn = () => {};
      const state = {
        fn
      };
      const result1 = selector(state);

      expect(selector({
        fn
      })).to.be.eql(result1); // the same function should be returned, when the same id and fn are passed
    });

    it('result function should be changed, if different fn is provided', () => {
      const fn = () => {};
      const state = {
        fn
      };
      const result1 = selector(state);

      expect(selector({
        fn: () => {}
      })).to.be.not.eql(result1);
    });
  });
});
