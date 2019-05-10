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
import React, { Component } from 'react';
import PropTypes from 'prop-types';
import invariant from 'invariant';
import { noop } from 'lodash';
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
    const {fields, error, children, handleSubmit, onSubmit, settingId} = this.props;

    // declare POST so that in case something goes terribly wrong and the browser handles the form
    // we don't put the params in the user-visible URL (e.g. the password when logging in)
    // (chris saw this once!)
    return (
      <form
        method='POST'
        onSubmit={onSubmit ? handleSubmit(onSubmit) : null}
        style={{...styles.innerForm, ...this.props.style}}
        data-qa={settingId}
        ref='form'>
        {error && <Message messageType='error'
          message={error.message}
          messageId={error.id}/>}

        {React.Children.map(children, (child) => {
          // will throw an error, if error has unsupported type for a component
          // I step in that case with FieldWithError component, that expect string as error, not an
          // object
          return React.cloneElement(child, { fields, handleSubmit, error });
        })}
      </form>
    );
  }
}


/**
 * Redux-form section that works with {@see connectComplexForm}
 * @typedef {Object} FormSection
 * @property {function: string[]} getFields - a method that returns a list of form field names
 * @property {function(): object} getInitialValues - a method that returns the initial values for fields
 * @property {function} validate - local form validation function
 * @property {function(state: object, formOwnProps: object)} formMapStateToProps - a function that called
 * against redux state and **form's** own properties, **not** section own properties.
 *
 * So if we have
 * ~~~
 * const formProps = { a: 1 };
 * const sectionProps = { b: 2 };
 * <Form {...formProps}>
 *    <Section {...sectionProps} />
 * </Form>
 * ~~~
 * then **formMapStateToProps** will receive **formProps**, despite the fact that formMapStateToProps is defined
 * in **Section** component
 *
 * @property {function(formValues: object, formWonProps: object): void} mutateSubmitValues - a function that mutates submit values
 */

/**
 * Creates HOC, that adds all fields that are in {@see sections} list to redux-form.
 *
 * Each section may have the following member functions ({@see FormSection}):
 *  1) fields
 *  2) initial values
 *  3) validation
 *  4) formMapStateToProps
 *  5) mutateSubmitValues
 *
 * @export
 * @param {*} [reduxFormParams={}]
 * @param {FormSection[]} [sections=[]]
 * @param {*} mapStateToProps
 * @param {*} mapDispatchToProps
 * @returns
 */
export function connectComplexForm(reduxFormParams = {}, sections = [], mapStateToProps, mapDispatchToProps) {
  const reduxFormDefaults = {
    // prevents validation call after losing focus on field
    touchOnBlur: false
  };
  const formParams = {
    ...reduxFormDefaults,
    ...reduxFormParams
  };
  const {
    getFields,
    getInitialValues,
    validate,
    formMapStateToProps: complexMapStateToProps,
    mutateSubmitValues
  } = mergeFormSections({ formMapStateToProps: mapStateToProps, ...formParams }, ...sections);

  const fields = (formParams.fields ? formParams.fields : []).concat(getFields());

  const initialValues = merge({},
    formParams.initialValues,
    getInitialValues()
  );

  const mapStateToPropsForDirtyWatcher = function(state, ownProps) {
    let propsInitialValues = ownProps.initialValues;
    const props = complexMapStateToProps(...arguments) || {};
    // a part of initial values could be static for the form,
    // and other part could come from complexMapStateToProps
    propsInitialValues = props.initialValues ?
      merge({}, propsInitialValues, props.initialValues)
      : propsInitialValues;
    const finalProps = {
      ...props,
      initialValues: propsInitialValues
    };
    finalProps.initialValuesForDirtyStateWatcher = propsInitialValues;
    return finalProps;
  };

  const complexForm = (component) => {
    return reduxForm({
      ...formParams, fields, validate, initialValues
    }, mapStateToPropsForDirtyWatcher, mapDispatchToProps)(wrapSubmitValueMutator(mutateSubmitValues, component));
  };

  return (component) => {
    const conflictDetectionComponent = ConflictDetectionWatcher(component);
    const dirtyWatchedComponent = FormDirtyStateWatcher(conflictDetectionComponent);
    return hoistNonReactStatic(complexForm(dirtyWatchedComponent), component);
  };
}

/**
 * Merge several form sections into a single section
 *
 * @param {FormSection} sections
 * @returns {FormSection} merged form section interface
 */
// export for tests
export const mergeFormSections = (...sections) => {
  const getFields = () => [].concat(flatten(sections.map(section =>
    section.getFields ? section.getFields() : []))
  );

  const getInitialValues = mergeFormSectionFunc(sections, 'getInitialValues');
  const validate = mergeFormSectionFunc(sections, 'validate');
  const formMapStateToProps = mergeFormSectionFunc(sections, 'formMapStateToProps');
  const mutateSubmitValues = mergeFormSectionFunc(sections, 'mutateSubmitValues');
  return {
    getFields,
    getInitialValues,
    validate,
    formMapStateToProps,
    mutateSubmitValues
  };
};

/**
 * Makes a function that deeply merges the results of {@see funcName} calls for each object in {@see sections}.
 * @param {object[]} sections - sections
 * @param {string} funcName - a function name, results of which should be merged
 * @returns {function: object} a deeply merged result
 */
// exported for testing
export const mergeFormSectionFunc = (sections, funcName) =>  {
  invariant(sections, 'section must be defined');
  invariant(funcName, 'funcName must be defined');

  if (sections.length === 0) {
    return noop;
  }

  return function() {
    return sections.reduce((obj, section) => {
      if (!section[funcName]) {
        return obj;
      }
      return merge(obj, section[funcName](...arguments));
    }, {});
  };
};

/**
 * {@see FormSection} api key names. {@see mergeFormSections} without parameter returns FormSection
 * with noop implementation
 */
const FormSectionApiKeys = Object.keys(mergeFormSections());

/**
 * Extract {@see FormSection} interface form {@see section}.
 *
 * A returned object is a new object that contains all FormSection api methods
 * @param {FormSection} section
 */
const extractFormSectionInterface = section => FormSectionApiKeys.reduce((formSectionApi, apiKey) => {
  formSectionApi[apiKey] = section[apiKey];
  return formSectionApi;
}, {});

/**
 * A decorator that merges current component FormSection interface with child sections
 * @param {...FormSection} childSections that current component contains
 */
export const sectionsContainer = (...childSections) => targetComponent => {
  // We should not pass original {@see targetComponent} to avoid infinite recursion. So extract
  // FormSection interface from targetComponent
  const formSection = extractFormSectionInterface(targetComponent);
  return Object.assign(targetComponent, mergeFormSections(formSection, ...childSections)); // eslint-disable-line no-restricted-properties
};

const styles = {
  innerForm: {
    display: 'flex',
    flexWrap: 'wrap'
  }
};

