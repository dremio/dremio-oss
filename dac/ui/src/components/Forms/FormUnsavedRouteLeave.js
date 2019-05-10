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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { showUnsavedChangesConfirmDialog } from 'actions/confirmation';
import { isUnauthorisedReason } from 'store/authMiddleware';
import { withRouter } from 'react-router';

/**
 * Returns specified form component wrapped into component preset with hooks to get confirmation dialog
 * when leaving route
 *
 * Note: normally you have to use default export, this function exists mostly for unit tests, since
 * default export is the same component but connected with redux
 *
 * @param  {React.Component} FormController
 * @return {React.Component} wrapped component
 */
export function wrapUnsavedChangesWithForm(FormController) {
  const Wrapped = withRouter(FormController);
  return class extends Component {
    static contextTypes = {
      router: PropTypes.object.isRequired
    };

    static propTypes = {
      route: PropTypes.object.isRequired,
      showUnsavedChangesConfirmDialog: PropTypes.func
    }

    state = {
      isFormDirty: false,
      ignoreUnsavedChanges: false
    };

    /**
     * Holds map of child forms and their dirty state, where key is child id and value is dirty state value.
     * This map updates only when setChildDirtyState used.
     *
     * @example
     * {
     *   'form1': false, // `false` means that child is pristine
     *   'form2': true   // `true` means that child is dirty
     * }
     * @type {Object}
     */
    childDirtyStates = {}

    componentDidMount() {
      this.context.router.setRouteLeaveHook(this.props.route, this.routeWillLeave);
    }

    leaveConfirmed(nextLocation) {
      this.setState({ignoreUnsavedChanges: true}, () => this.context.router.push(nextLocation));
    }

    routeWillLeave = (nextLocation) => {
      if (this.state.isFormDirty && !isUnauthorisedReason(nextLocation) && !this.state.ignoreUnsavedChanges) {
        this.props.showUnsavedChangesConfirmDialog({
          confirm: () => this.leaveConfirmed(nextLocation)
        });
        return false;
      }
      return true;
    }

    updateFormDirtyState = (isFormDirty) => {
      this.setState({isFormDirty});
    }

    hasDirtyChild() {
      return Object.values(this.childDirtyStates).some(dirty => dirty);
    }

    /**
     * Set dirty state by specified child id. Returns function where dirty state will be recieved in first argument.
     * You need to use this method for cases when you have several form inside of component and you need to track
     * dirty state for all forms at once. This method should be passed to `updateFormDirtyState` prop to Form component
     * and this Form component should be wrapped in `connectComplexForm`.
     *
     * @example
     * class MultiFormComponent extends Component {
     *   static propTypes = {
     *     setChildDirtyState: PropTypes.func
     *   }
     *
     *   render() {
     *     return (
     *       <div>
     *         <SomeConnectedForm updateFormDirtyState={this.props.setChildDirtyState('form1')} />
     *         <SomeConnectedForm updateFormDirtyState={this.props.setChildDirtyState('form2')} />
     *       </div>
     *     );
     *   }
     * }
     *
     * export default FormUnsavedRouteLeave(MultiFormComponent);
     *
     * @param {string} formId child form id should be unique
     */
    setChildDirtyState = (formId) => (dirty) => {
      this.childDirtyStates[formId] = dirty;
      this.updateFormDirtyState(this.hasDirtyChild());
    }

    render() {
      return <Wrapped {...this.props}
        updateFormDirtyState={this.updateFormDirtyState}
        setChildDirtyState={this.setChildDirtyState}
       />;
    }
  };
}

export default function FormUnsavedRouteLeave(FormController) {
  return connect(null, { showUnsavedChangesConfirmDialog })(wrapUnsavedChangesWithForm(FormController));
}
