/*
 * Copyright (C) 2017 Dremio Corporation
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
import React, { Component, PropTypes } from 'react';
import { reduxForm, propTypes as formPropTypes } from 'redux-form';
import flatten from 'lodash/flatten';
import { merge } from 'lodash/object';
import hoistNonReactStatic from 'hoist-non-react-statics';

import Message from 'components/Message';

import FormDirtyStateWatcher from './FormDirtyStateWatcher';
import ConflictDetectionWatcher from './ConflictDetectionWatcher';
import wrapSubmitValueMutator from './wrapSubmitValueMutator';

export class InnerComplexForm extends Component {
  static propTypes = {
    onSubmit: PropTypes.func,
    ...formPropTypes
  };

  render() {
    const {fields, error, children, handleSubmit, onSubmit} = this.props;

    // declare POST in case something goes terribly wrong and the browser handles the form
    // we don't put the params in the user-visible URL (e.g. the password when logging in)
    // (chris saw this once!)
    return (
      <form
        method='POST'
        onSubmit={onSubmit ? handleSubmit(onSubmit) : null}
        style={{...styles.innerForm, ...this.props.style}}
        ref='form'>
        {error && <Message messageType='error'
          message={error.message}
          messageId={error.id}/>}

        {React.Children.map(children, (child) => {
          return React.cloneElement(child, { fields, handleSubmit });
        })}
      </form>
    );
  }
}

export function connectComplexForm(reduxFormParams = {}, sections = [], mapStateToProps, mapDispatchToProps) {
  const reduxFormDefaults = {
    // prevents validation call after losing focus on field
    touchOnBlur: false
  };
  const formParams = {
    ...reduxFormDefaults,
    ...reduxFormParams
  };
  const fields = (formParams.fields ? formParams.fields : []).concat(
    flatten(sections.map(section =>
      section.getFields ? section.getFields() : [])
    )
  );

  const initialValues = {
    ...formParams.initialValues,
    ...sections.reduce((obj, section) => {
      return {...obj, ...(section.getInitialValues && section.getInitialValues())};
    }, {})
  };

  function validate() {
    return merge(
      {},
      formParams.validate && formParams.validate(...arguments),
      sections.reduce((obj, section) => {
        return merge(obj, section.validate ? section.validate(...arguments) : {});
      }, {})
    );
  }

  function complexMapStateToProps() {
    return merge(
      {},
      mapStateToProps && mapStateToProps(...arguments),
      sections.reduce((obj, section) => {
        return merge(obj, section.mapStateToProps ? section.mapStateToProps(...arguments) : {});
      }, {})
    );
  }

  function mutateSubmitValues(values) {
    formParams.mutateSubmitValues && formParams.mutateSubmitValues(values);
    sections.forEach(section => {
      section.mutateSubmitValues && section.mutateSubmitValues(values);
    });
  }

  const mapStateToPropsForDirtyWatcher = function(state, ownProps) {
    let propsInitialValues = ownProps.initialValues;
    const props = complexMapStateToProps(...arguments) || {};
    propsInitialValues = props.initialValues || propsInitialValues;
    props.initialValuesForDirtyStateWatcher = propsInitialValues;
    return props;
  };

  const complexForm = (component) => {
    return reduxForm({
      ...formParams, fields, validate, initialValues
    }, mapStateToPropsForDirtyWatcher, mapDispatchToProps)(wrapSubmitValueMutator(mutateSubmitValues, component));
  };

  if (formParams.disableStateWatcher) {
    return complexForm;
  }

  return (component) => {
    const conflictDetectionComponent = ConflictDetectionWatcher(component);
    const dirtyWatchedComponent = FormDirtyStateWatcher(conflictDetectionComponent);
    return hoistNonReactStatic(complexForm(dirtyWatchedComponent), component);
  };
}

const styles = {
  innerForm: {
    display: 'flex',
    flexWrap: 'wrap'
  }
};

