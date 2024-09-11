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
import { Component } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { showUnsavedChangesConfirmDialog } from "actions/confirmation";
import { clearDataBetweenTabs } from "actions/modals/passDataBetweenTabs";

/**
 * Returns specified Modal component wrapped into component which tracks and warn user about unsaved changes
 *
 * Note: normally you have to use default export, this function exists mostly for unit tests, since
 * default export is the same component but connected with redux
 *
 * @param  {React.Component} Modal Modal component to wrap
 * @return {React.Component} wrapped Modal component
 */
export function wrapUnsavedChangesWarningWithModal(Modal) {
  return class extends Component {
    static propTypes = {
      hide: PropTypes.func,
      //connected
      showUnsavedChangesConfirmDialog: PropTypes.func,
      clearDataBetweenTabs: PropTypes.func,
    };

    state = {
      isFormDirty: false,
    };

    handleHide = (promiseResolver) => {
      this.props.hide();
      this.updateFormDirtyState(false);
      promiseResolver(true);
    };

    /**
     * Hide modal with appropriate check for dirty state and show warning message regarding unsaved
     * changes when form is dirty. During submit you have to call this method by yourself and pass formSubmitted
     * flag with `true` value to notify that form is submitted and no warning needed. Usually this method called
     * as success callback to ApiUtils.attachFormSubmitHandlers promise.
     *
     * @example
     * submit(formValues) {
     *   return ApiUtils.attachFormSubmitHandlers(formValues).then(() => this.props.hide(null, true));
     * }
     *
     * @param  {object} [event]
     * @param  {boolean} [formSubmitted] `true` when form is submitted
     * @returns {Promise} a promise which is resolved with {@see true}, if a modal close is confirmed,
     * {@see false} otherwise
     */
    hide = (event, formSubmitted) => {
      return new Promise((resolve) => {
        // strict check for non-true value because method arguments may vary and contains additional
        // event objects as second arguments which supposed to be formSubmitted and responds as truthy
        if (this.state.isFormDirty && formSubmitted !== true) {
          this.props.showUnsavedChangesConfirmDialog({
            confirm: () => this.handleHide(resolve),
            cancel: () => resolve(false),
          });
        } else {
          this.handleHide(resolve);
        }
        this.props.clearDataBetweenTabs();
      });
    };

    updateFormDirtyState = (isFormDirty) => {
      this.setState({ isFormDirty });
    };

    render() {
      return (
        <Modal
          {...this.props}
          updateFormDirtyState={this.updateFormDirtyState}
          isFormDirty={this.state.isFormDirty}
          hide={this.hide}
        />
      );
    }
  };
}

export default function FormUnsavedWarningHOC(Modal) {
  return connect(null, {
    showUnsavedChangesConfirmDialog,
    clearDataBetweenTabs,
  })(wrapUnsavedChangesWarningWithModal(Modal));
}
