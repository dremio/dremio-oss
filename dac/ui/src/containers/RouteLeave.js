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
import { Component, createContext, Fragment } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import uuid from "uuid";
import { isUnauthorisedReason } from "store/authMiddleware";
import { showConfirmationDialog } from "actions/confirmation";
import { resetQueryState } from "@app/actions/explore/view";
import { KeyChangeTrigger } from "@app/components/KeyChangeTrigger";

/**
 * @typedef {Object} ChangesCheckResult
 *
 * @property {bool} hasChanges - if {@see false}, then there is no changes and a user could proceed
 * if {@see true} that means that there are some changes and we should wait for
 * {@see ChangesCheckResult.userChoiceToLeaveOrStayPromise}, which is resolved with 'leaveTheChanges'
 * bool flag. We are showing a confirmation dialog to a user. 'leaveTheChanges' corresponds to a
 * choice of a user
 *
 * @property {Promise(true|false)} userChoiceToLeaveOrStayPromise - a promise which is resolved with
 * 'leaveTheChanges' bool flag
 */

/**
 * A context that provides a following api:
 * {
 *    // func(componentId: string, hasChangeCallback: (nextLocation) => bool): removeCallbackHandler () => void
 *    addCallback: func(componentId: string, hasChangeCallback: (nextLocation) => bool): removeCallbackHandler () => void,
 *    doChangesCheck: func(): {@see ChangesCheckResult}
 * }
 */
const { Provider, Consumer } = createContext();

/**
 * a context that provides a following api:
 * func(nextLocation): {@see ChangesCheckResult}
 */
export const performChangesCheckForLocationContext = createContext();

export class HookProviderView extends Component {
  static propTypes = {
    //title: PropTypes.string, // a title for confirm messaged
    route: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
    children: PropTypes.node,

    showConfirmationDialog: PropTypes.func.isRequired,
    resetQueryState: PropTypes.func,
  };

  constructor(props) {
    super(props);
    this.addCallback = this.addCallback.bind(this); // I have to do this in that way, instead of
    // using arrow function. I need to spy this function. To do this, it should be presented in
    // prototype. Arrow functions are not present in the prototype
  }

  hasChangeCallbacks = new Map(); // {string, hasChangesCallback() => bool}

  // add hasChangesCallback to local storage.
  addCallback(componentId, hasChangesCallback) {
    const map = this.hasChangeCallbacks;
    map.set(componentId, hasChangesCallback);
    return () => {
      // remove handler
      map.delete(componentId);
    };
  }

  /**
   * Performs a check for changes and shows a leave confirmation dialog, if there is any unsaved
   * change
   *
   * @param {object} nextLocation
   * @returns {ChangesCheckResult}
   *
   * @memberof HookProviderView
   */
  doChangesCheckForNextLocation = (nextLocation) => {
    const result = {
      hasChanges: this.checkChanges(nextLocation),
      userChoiceToLeaveOrStayPromise: null,
    };

    if (result.hasChanges) {
      result.userChoiceToLeaveOrStayPromise = new Promise((resolve) => {
        this.props.showConfirmationDialog({
          title: la("Unsaved Changes"),
          text: la("Are you sure you want to leave without saving changes?"),
          confirmText: la("Leave"),
          cancelText: la("Stay"),
          confirm: () => {
            this.props.resetQueryState();
            resolve(true);
          },
          cancel: () => resolve(false),
        });
      });
    }
    return result;
  };

  /**
   * Performs a check for changes and shows a leave confirmation dialog, if there is any unsaved
   * change. This method simulates a check similar to navigation out to a page that does not exists
   *
   * @returns {ChangesCheckResult}
   *
   * @memberof HookProviderView
   */
  doChangesCheck = () => {
    return this.doChangesCheckForNextLocation({
      pathname: "/not_existing_page",
      query: {},
    });
  };

  //requests hasChanges state from all subscribers
  checkChanges = (nextLocation) => {
    let hasChanges = false;

    this.hasChangeCallbacks.forEach((hasChangesCallback) => {
      if (hasChangesCallback(nextLocation)) {
        hasChanges = true;
      }
    });

    return hasChanges;
  };

  render() {
    return (
      <Provider
        value={{
          addCallback: this.addCallback,
          doChangesCheck: this.doChangesCheck,
        }}
      >
        <performChangesCheckForLocationContext.Provider
          value={this.doChangesCheckForNextLocation}
        >
          {this.props.children}
        </performChangesCheckForLocationContext.Provider>
      </Provider>
    );
  }
}

export const HookProvider = connect(null, {
  showConfirmationDialog,
  resetQueryState,
})(HookProviderView);

export const withHookProvider = (ComponentToWrap) => {
  class WithHookProvider extends Component {
    static propTypes = {
      route: PropTypes.object.isRequired,
      router: PropTypes.object.isRequired,
    };
    render() {
      return (
        <HookProvider route={this.props.route} router={this.props.router}>
          <ComponentToWrap {...this.props} />
        </HookProvider>
      );
    }
  }

  return WithHookProvider;
};

/* will return a function, that receives a SINGLE argument hasChangesCallback.
  This function will add generated id to a call of the method, which is provided by HookProvider
  input params for result function:
  state = {
    fn: (id, hasChangesCallback: () => bool ) =>
      [func(hasChangesCallback: () => bool) => [removeHasChangesCallback() => void]]
  }
  */
export const singleArgFnGenerator = (id, saveRemoveCallback) => {
  return createSelector(
    ({ fn }) => fn,
    (fn) => (hasChangesCallback) => {
      const removeCallback = fn(id, hasChangesCallback);

      saveRemoveCallback(removeCallback);
    }
  );
};

export class HookConsumer extends Component {
  static propTypes = {
    // ({ addCallback, doChangesCheck }) => {}. addCallback is a function that receives a SINGLE argument.
    // This argument is hasChangesCallback
    children: PropTypes.func,
  };

  // should be a new selector for each HookConsumer instance
  singleArgFnGetter = singleArgFnGenerator(
    uuid.v4(), // generate a unique id for each consumer.
    (callback) => {
      this.removeCallback = callback;
    }
  );
  removeCallback = null; // will store a remove callback

  renderFn = ({
    addCallback: twoArgsCallback /* (id, hasChangeCallback) => {} */,
    doChangesCheck,
  }) => {
    // in other words I transform a function that accepts 2 arguments (id and a callback) to
    // a function that accept a single argument (a callback),
    // but internally call original function with fixed generated id
    return this.props.children({
      addCallback: this.singleArgFnGetter({
        fn: twoArgsCallback,
      }),
      doChangesCheck,
    });
  };

  componentWillUnmount() {
    // we must clear callback for current component, if any exists
    if (this.removeCallback) {
      this.removeCallback();
      this.removeCallback = null;
    }
  }

  render() {
    return <Consumer>{this.renderFn}</Consumer>;
  }
}

//export for testing
export class RouteLeaveEventView extends Component {
  static propTypes = {
    route: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
    doChangesCheck: PropTypes.func.isRequired, // func(nextLocation): {@see ChangesCheckResult}
    children: PropTypes.node,
  };

  static contextType = performChangesCheckForLocationContext.Consumer;

  ignoreUnsavedChanges = false;

  onRouteChange = () => {
    const { router, route } = this.props;
    router.setRouteLeaveHook(route, this.routeWillLeave);
  };

  routeWillLeave = (nextLocation) => {
    if (this.ignoreUnsavedChanges || isUnauthorisedReason(nextLocation)) {
      this.ignoreUnsavedChanges = false; // reset a flag for a next try
      return true;
    }
    // doChangesCheck may open a popup, hence should not precede the previous 'if'
    const { hasChanges, userChoiceToLeaveOrStayPromise } =
      this.props.doChangesCheck(nextLocation);
    if (!hasChanges) {
      return true;
    }
    userChoiceToLeaveOrStayPromise
      .then((leaveTheChanges) => {
        if (leaveTheChanges) {
          this.ignoreUnsavedChanges = true;
          this.props.router.push(nextLocation);
        }
        return null;
      })
      .catch((err) => err);
    return false;
  };

  render() {
    const { route, children } = this.props;

    return (
      <Fragment>
        <KeyChangeTrigger
          key="trigger"
          keyValue={route}
          onChange={this.onRouteChange}
        />
        {children}
      </Fragment>
    );
  }
}

/**
 * a HOC that should be applied to bottom level routes to trigger changes check on route change
 */
export const withRouteLeaveEvent = (ComponentToWrap) => {
  return class extends Component {
    static propTypes = {
      route: PropTypes.object.isRequired,
      router: PropTypes.object.isRequired,
    };

    static contextType = performChangesCheckForLocationContext.Consumer;

    ignoreUnsavedChanges = false;

    onRouteChange = () => {
      const { router, route } = this.props;
      router.setRouteLeaveHook(route, this.routeWillLeave);
    };

    routeWillLeave = (nextLocation) => {
      if (!this.ignoreUnsavedChanges && !isUnauthorisedReason(nextLocation)) {
        this.context(nextLocation)
          .then((leaveTheChanges) => {
            if (leaveTheChanges) {
              this.ignoreUnsavedChanges = true;
              this.props.router.push(nextLocation);
            }
            return null;
          })
          .catch((err) => err);
        return false;
      }
      this.ignoreUnsavedChanges = false; // reset a flag for a next try
      return true;
    };

    render() {
      return (
        <performChangesCheckForLocationContext.Consumer>
          {(doChangesCheck) => (
            <RouteLeaveEventView
              route={this.props.route}
              router={this.props.router}
              doChangesCheck={doChangesCheck}
            >
              <ComponentToWrap key="component" {...this.props} />
            </RouteLeaveEventView>
          )}
        </performChangesCheckForLocationContext.Consumer>
      );
    }
  };
};

// adds a callback to the props, that force a current route to subscribe on component changes.
// Call this callback with another function that returns bool value, that indicates,
// if wrapped component has changes.
export const withRouteLeaveSubscription = (
  ComponentToWrap,
  /* optional */ fieldName
) => {
  return class extends Component {
    renderFn = ({ addCallback, doChangesCheck }) => {
      const newProps = {
        ...this.props,
        [fieldName || "addHasChangesHook"]: addCallback,
        doChangesCheck,
      };
      return <ComponentToWrap {...newProps} />;
    };

    render() {
      return <HookConsumer>{this.renderFn}</HookConsumer>;
    }
  };
};
