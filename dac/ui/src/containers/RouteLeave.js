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
import { Component, createContext } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { createSelector } from 'reselect';
import uuid from 'uuid';
import { isUnauthorisedReason } from 'store/authMiddleware';
import { showConfirmationDialog } from 'actions/confirmation';

const {
  Provider,
  Consumer
// the function below has a following signature
// (componentId: string, hasChangeCallback: (nextLocation) => bool): removeCallbackHandler () => void;
} = createContext(/* func */ null);

export class HookProviderView extends Component {
  static propTypes = {
    //title: PropTypes.string, // a title for confirm messaged
    route: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
    children: PropTypes.node,

    showConfirmationDialog: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
    this.addCallback = this.addCallback.bind(this); // I have to do this in that way, instead of
    // using arrow function. I need to spy this function. To do this, it should be presented in
    // prototype. Arrow functions are not present in the prototype
  }

  hasChangeCallbacks = new Map(); // {string, hasChangesCallback() => bool}
  ignoreUnsavedChanges = false;

  // add hasChangesCallback to local storage.
  addCallback(componentId, hasChangesCallback) {
    const map = this.hasChangeCallbacks;
    map.set(componentId, hasChangesCallback);
    return () => { // remove handler
      map.delete(componentId);
    };
  }

  //requests hasChanges state from all subscribers
  checkChanges = (nextLocation) => {
    let hasChanges = false;

    this.hasChangeCallbacks.forEach((hasChangesCallback, key) => {
      if (hasChangesCallback(nextLocation)) {
        hasChanges = true;
      }
    });

    return hasChanges;
  }

  leaveConfirmed = (nextLocation) => {
    this.ignoreUnsavedChanges = true;
    this.props.router.push(nextLocation);
  }

  routeWillLeave = (nextLocation) => {
    if (!this.ignoreUnsavedChanges && !isUnauthorisedReason(nextLocation) && this.checkChanges(nextLocation)) {
      this.props.showConfirmationDialog({
        title: la('Unsaved Changes'),
        text: la('Are you sure you want to leave without saving changes?'),
        confirmText: la('Leave'),
        cancelText: la('Stay'),
        confirm: () => this.leaveConfirmed(nextLocation)
      });
      return false;
    }
    this.ignoreUnsavedChanges = false; // reset a flag for a next try
    return true;
  }

  componentDidMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(/* nextProps */ {
      route,
      router
  }, prevProps = {}) {
    if (route !== prevProps.route) { // if route is changed, we should put a hook to a new route
      this.props.router.setRouteLeaveHook(route, this.routeWillLeave);
    }
  }

  render() {
    return <Provider value={this.addCallback}>
      {this.props.children}
    </Provider>;
  }
}

export const HookProvider = connect(null, { showConfirmationDialog })(HookProviderView);

export const withHookProvider = ComponentToWrap => {
  class WithHookProvider extends Component {
    static propTypes = {
      route: PropTypes.object.isRequired,
      router: PropTypes.object.isRequired
    }
    render() {
      return (<HookProvider route={this.props.route} router={this.props.router}>
        <ComponentToWrap {...this.props} />
      </HookProvider>);
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
  return createSelector(({ fn }) => fn, (fn) => hasChangesCallback => {
    const removeCallback = fn(id, hasChangesCallback);

    saveRemoveCallback(removeCallback);
  });
};

export class HookConsumer extends Component {
  static propTypes = {
    // (addCallback) => {}. addCallback is a function that receives a SINGLE argument.
    // This argument is hasChangesCallback
    children: PropTypes.func
  }

  // should be a new selector for each HookConsumer instance
  singleArgFnGetter = singleArgFnGenerator(uuid.v4(), // generate a unique id for each consumer.
    (callback) => {
      this.removeCallback = callback;
    });
  removeCallback = null; // will store a remove callback

  renderFn = twoArgsCallback /* (id, hasChangeCallback) => {} */ => {

    // in other words I tansform a function that accepts 2 arguments (id and a callback) to
    // a function that accept a single argument (a callback),
    // but internally call original function with fixed generated id
    return this.props.children(this.singleArgFnGetter({
      fn: twoArgsCallback
    }));
  }

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


// adds a callback to the props, that force a current route to subscribe on component changes.
// Call this callback with another function that returns bool value, that indicates,
// if wrapped component has changes.
export const withRouteLeaveSubscription = (ComponentToWrap, /* optional */ fieldName) => {
  return class extends Component {
    renderFn = addCallback => {
      const newProps = {
        ...this.props,
        [fieldName || 'addHasChangesHook']: addCallback
      };
      return <ComponentToWrap {...newProps} />;
    }

    render() {
      return <HookConsumer>{this.renderFn}</HookConsumer>;
    }
  };
};
