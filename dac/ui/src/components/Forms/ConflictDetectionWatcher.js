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
import { showConflictConfirmationDialog } from 'actions/confirmation';

/**
 * Returns specified Form component wrapped into component which tracks and warn user about lost data
 * caused by form updates from another session. This wrapper will track changes for `version` field
 * and if it exists will show confirmation modal when field changed.
 *
 * Note: normally you have to use default export, this function exists mostly for unit tests, since
 * default export is the same component but connected with redux
 *
 * @param  {React.Component} Form Form component to wrap
 * @return {React.Component} wrapped Form component
 */
export function wrapConflictDetectionForm(Form) {
  return class extends Component {
    static propTypes = {
      /**
       * Optional function to get value from props and compare it with value from next props to determine
       * if we need to warn user about form changes.
       * Following arguments will be passed in:
       * - props - Component props
       *
       * @type {function}
       */
      getConflictedValues: PropTypes.func,
      showConflictConfirmationDialog: PropTypes.func,
      submitting: PropTypes.bool,
      values: PropTypes.object
    }

    componentWillReceiveProps(nextProps) {
      if (!nextProps.getConflictedValues || nextProps.submitting) {
        return;
      }
      if (this.isConflictedValueChanged(nextProps, this.props)) {
        nextProps.showConflictConfirmationDialog();
      }
    }

    /**
     * Compares values between new and old props using getConflictedValues to get actual values.
     * When one of the values is undefined `false` will be returned.
     * Currently it supports only primitive values.
     *
     * @param  {object} nextProps
     * @param  {object} oldProps
     * @return {boolean} true when equals
     */
    isConflictedValueChanged(nextProps, oldProps) {
      const newValue = this.props.getConflictedValues(nextProps);
      const oldValue = this.props.getConflictedValues(oldProps);
      if (newValue === undefined || oldValue === undefined) {
        return false;
      }
      return newValue !== oldValue;
    }

    render() {
      return <Form {...this.props} />;
    }
  };
}

export default function ConflictDetectionWatcher(Form) {
  return connect(null, { showConflictConfirmationDialog })(wrapConflictDetectionForm(Form));
}
