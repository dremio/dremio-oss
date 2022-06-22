/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { shallow, mount } from "enzyme";
import PropTypes from "prop-types";
import { KeyChangeTrigger } from "@app/components/KeyChangeTrigger";
import {
  HookProviderView,
  HookConsumer,
  singleArgFnGenerator,
  RouteLeaveEventView,
} from "./RouteLeave";

const routeProps = {
  route: {
    path: "/new_query",
  },
  router: {
    setRouteLeaveHook: () => {},
    push: () => {},
  },
};

describe("RouteLeave.js", () => {
  const providerCommonProps = {
    ...routeProps,
    showConfirmationDialog: () => {},
  };

  describe("RouteLeaveEventView", () => {
    const eventCompProps = {
      ...routeProps,
      doChangesCheck: () => Promise.resolve(true),
    };
    it("calls onRouteChange on route change", () => {
      const wrapper = shallow(<RouteLeaveEventView {...eventCompProps} />);
      const trigger = wrapper.find(KeyChangeTrigger);
      expect(trigger).to.have.length(1);
      expect(trigger.prop("keyValue")).to.be.equal(
        routeProps.route,
        "route must be monitored"
      );
      expect(trigger.prop("onChange")).to.be.equal(
        wrapper.instance().onRouteChange
      );
    });

    it("onRouteChange sets a route leave hook", () => {
      const props = {
        ...eventCompProps,
        router: {
          setRouteLeaveHook: sinon.spy(),
        },
      };
      const wrapper = shallow(<RouteLeaveEventView {...props} />);
      const instance = wrapper.instance();

      instance.onRouteChange();
      expect(props.router.setRouteLeaveHook).to.be.calledWith(
        props.route,
        instance.routeWillLeave
      );
    });

    it("routeWillLeave returns true, if leave was confirmed", async () => {
      const userChoiceToLeaveOrStayPromise = Promise.resolve(true);
      const doCheckFn = sinon.stub();
      doCheckFn.returns({
        hasChanges: true,
        userChoiceToLeaveOrStayPromise,
      });

      const props = {
        ...eventCompProps,
        doChangesCheck: doCheckFn,
      };
      const instance = shallow(<RouteLeaveEventView {...props} />).instance();

      expect(instance.routeWillLeave()).to.eql(false); // should not allow to leave a page;
      expect(doCheckFn).to.be.called;
      doCheckFn.resetHistory();

      await userChoiceToLeaveOrStayPromise;

      expect(instance.routeWillLeave()).to.eql(true); // should allow to leave a page, as there was a confirmation
      expect(doCheckFn).to.be.not.called;
    });

    it("routeWillLeave returns true, if redirect reason is unauthorized", () => {
      const instance = shallow(
        <RouteLeaveEventView {...eventCompProps} />
      ).instance();

      expect(instance.routeWillLeave({ search: "abc&reason=401" })).to.eql(
        true
      );
    });
  });

  describe("HookProviderView", () => {
    let commonProps;

    beforeEach(() => {
      commonProps = providerCommonProps;
    });

    describe("showConfirmationDialog", () => {
      let confirmFn;
      let instance;

      beforeEach(() => {
        confirmFn = sinon.spy();
        instance = shallow(
          <HookProviderView
            {...commonProps}
            showConfirmationDialog={confirmFn}
          />
        ).instance();
      });

      it("is not called, when there is no changes", () => {
        const hasChangesCallback = () => false; // no changes

        instance.addCallback("test id", hasChangesCallback);
        instance.doChangesCheck(); // simulate route change

        expect(confirmFn).to.be.not.called; // confirm function should NOT be called
      });

      it("reacts on hasChangesCallback result change", () => {
        const hasChangesCallback = sinon.stub();
        instance.addCallback("test id", hasChangesCallback);

        const changeValue = (hasChanges) => {
          hasChangesCallback.returns(true); // change the result
          instance.doChangesCheck(); // simulate route change
          if (hasChanges) {
            expect(confirmFn).to.be.called;
          } else {
            expect(confirmFn).to.not.be.called;
          }
        };

        changeValue(true);
        //check transitions
        // Todo: Fix the below UT (https://dremio.atlassian.net/browse/DX-30942)
        // changeValue(false); // true -> false
        changeValue(true); // false -> true
      });

      it("not called if hasChangesCallback was removed", () => {
        const hasChangesCallback = () => true; // simulate changes

        const removeHandler = instance.addCallback(
          "test id",
          hasChangesCallback
        );
        instance.doChangesCheck(); // simulate route change

        expect(confirmFn).to.be.called; // confirm function should be called if there is any change

        removeHandler();

        confirmFn.resetHistory();
        instance.doChangesCheck(); // simulate route change
        expect(confirmFn).to.be.not.called; // should Not be called, as callback is removed and not monitored anymore
      });

      it("is called, when at least one hasChangesCallback returns true", () => {
        const hasChangesCalbackList = Array(5).fill(false);
        const removeHandlers = {};
        const trueIndex = 2;
        hasChangesCalbackList[trueIndex] = true; // third element has changes

        // add all callbacks
        hasChangesCalbackList.forEach((hasChanges, index) => {
          removeHandlers[index] = instance.addCallback(
            `test id ${index}`,
            () => hasChanges
          );
        });
        instance.doChangesCheck(); // simulate route change

        expect(confirmFn).to.be.called;

        removeHandlers[trueIndex](); // remove a handler, that has changes

        confirmFn.resetHistory();
        instance.doChangesCheck(); // simulate route change
        expect(confirmFn).to.be.not.called; // should not be called as the reset of handlers does not have changes
      });
    });
  });

  describe("HookConsumer", () => {
    const getRenderFn =
      (hasChangesCallback) =>
      // eslint-disable-next-line
      ({ addCallback }) => {
        addCallback(hasChangesCallback);
        return <div></div>;
      };

    let addCallbackOriginal;
    beforeEach(() => {
      sinon.spy(HookProviderView.prototype, "addCallback");
      addCallbackOriginal = HookProviderView.prototype.addCallback;
    });

    afterEach(() => {
      addCallbackOriginal.restore();
      addCallbackOriginal = null;
    });

    it("Original Provider.addCallback is called", () => {
      const hasChanges = () => true;
      const renderFn = getRenderFn(hasChanges);

      mount(
        <HookProviderView {...providerCommonProps}>
          <HookConsumer>{renderFn}</HookConsumer>
        </HookProviderView>
      );

      expect(addCallbackOriginal).to.be.called;
      const [id, argFn] = addCallbackOriginal.lastCall.args;
      expect(id).to.exist; // not empty id should be generated
      expect(argFn).to.eql(hasChanges); // addCallback should be called with provided hasChanges function
    });

    it("Unique ids are generated for different consumers ", () => {
      mount(
        <HookProviderView {...providerCommonProps}>
          <div>
            <HookConsumer>{getRenderFn()}</HookConsumer>
            <HookConsumer>{getRenderFn()}</HookConsumer>
          </div>
        </HookProviderView>
      );

      expect(addCallbackOriginal).to.be.calledTwice; // should be called 2 times, as there are 2 consumers
      const id1 = addCallbackOriginal.args[0][0];
      const id2 = addCallbackOriginal.args[1][0];
      expect(id1).not.eql(id2); // uninque ids should be generated by each consumer
    });

    it("hasChanges callback is removed on unmount", () => {
      const hasChanges = () => true;
      let provider;
      const providerRef = (p) => {
        provider = p;
      };
      const TestComp = ({ mounted, refFn }) => {
        return (
          <HookProviderView {...providerCommonProps} ref={refFn}>
            {mounted && <HookConsumer>{getRenderFn(hasChanges)}</HookConsumer>}
          </HookProviderView>
        );
      };
      TestComp.propTypes = {
        mounted: PropTypes.bool,
        refFn: PropTypes.func,
      };
      const wrapper = mount(<TestComp mounted refFn={providerRef} />);
      expect(provider.hasChangeCallbacks).to.have.property("size", 1); // one callback should be added

      wrapper.setProps({ mounted: false }); // unmount consumer
      expect(provider.hasChangeCallbacks).is.empty; // callback should be removed from the list
    });
  });

  // these test are very important from performance point of view.
  // singleArgFnGenerator should return the same result if parameters are not changed, to avoid unnecessary rerenders.
  describe("singleArgFnGenerator", () => {
    let selector;
    const id = "test id";
    beforeEach(() => {
      selector = singleArgFnGenerator(id, () => {});
    });

    it("wraps a fn correctly", () => {
      const callback1 = sinon.spy();
      const state = {
        fn: callback1,
      };
      const hasChangesCallback = () => {};

      const resultFn = selector(state);
      resultFn(hasChangesCallback);

      expect(callback1).to.be.calledWith(id, hasChangesCallback); // result function should add id as first argument to original function
    });

    it("result function should return the same function, if input values are the same", () => {
      const fn = () => {};
      const state = {
        fn,
      };
      const result1 = selector(state);

      expect(
        selector({
          fn,
        })
      ).to.be.eql(result1); // the same function should be returned, when the same id and fn are passed
    });

    it("result function should be changed, if different fn is provided", () => {
      const fn = () => {};
      const state = {
        fn,
      };
      const result1 = selector(state);

      expect(
        selector({
          fn: () => {},
        })
      ).to.be.not.eql(result1);
    });
  });
});
