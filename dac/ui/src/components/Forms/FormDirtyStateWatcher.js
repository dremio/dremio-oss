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
import Immutable from 'immutable';
import { get } from 'lodash';


export default function FormDirtyStateWatcher(Form) {
  return class extends Component {
    static propTypes = {
      dirty: PropTypes.bool,
      values: PropTypes.object,
      initialValuesForDirtyStateWatcher: PropTypes.object,
      updateFormDirtyState: PropTypes.func
    };

    static defaultProps = {
      values: {}
    };

    state = {
      dirty: false
    };

    //Jackson serialization: when value does not exist, it is processed as undefined
    //We want to remove these undefined values to make check for deep equality
    //So undefined values are not the only differences between them
    _removeKeysWithUndefinedValue(valuesList) {
      return valuesList.map((itemValues) => {
        const clean = {};
        // undefined itemValues should be parsed as null if they ever occur
        if (itemValues === undefined) {
          return null;
        }
        // keep non-objects and nulls as they are
        if (itemValues === null || typeof itemValues !== 'object') {
          return itemValues;
        }
        Object.keys(itemValues).forEach(key => {
          if (itemValues[key] !== undefined) {
            clean[key] = itemValues[key];
          }
        });
        return clean;
      });
    }

    areArrayFieldsDirty(nextProps) {
      // fields in props used as initial value for array field
      // and tracks field's dirty state because of issue in redux-form
      // todo: remove its usage when redux form will be updated to v6
      const areFieldsEqual = (a, b) => Immutable.fromJS(a).equals(Immutable.fromJS(b));
      return Object.keys(nextProps.values).some((field) => {
        // allow skipping of dirty check for certain fields
        if (nextProps.skipDirtyFields && nextProps.skipDirtyFields.includes(field)) {
          return false;
        }

        if (!Array.isArray(nextProps.values[field])) return false;

        const currentValue = nextProps.values[field];
        const initialValue = get(nextProps.initialValuesForDirtyStateWatcher, field, []); // fallback for creation forms
        return !areFieldsEqual(this._removeKeysWithUndefinedValue(currentValue),
                               this._removeKeysWithUndefinedValue(initialValue));
      });
    }

    componentWillReceiveProps(nextProps) {
      const dirty = nextProps.dirty || this.areArrayFieldsDirty(nextProps);
      if (this.state.dirty !== dirty) {
        if (this.props.updateFormDirtyState) {
          this.props.updateFormDirtyState(dirty);
        }
        this.setState({ dirty });
      }
    }

    render() {
      return <Form {...this.props} dirty={this.state.dirty} />;
    }
  };
}
