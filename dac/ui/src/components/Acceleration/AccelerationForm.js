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
import ReactDOM from 'react-dom';
import { connectComplexForm, FormBody, FormTitle, ModalForm, modalFormProps } from 'components/Forms';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import uuid from 'uuid';
import deepEqual from 'deep-equal';

import reflectionActions from 'actions/resources/reflection';
import { getListErrorsFromNestedReduxFormErrorEntity } from 'utils/validation';
import {
  areReflectionFormValuesBasic,
  areReflectionFormValuesUnconfigured,
  createReflectionFormValues,
  fixupReflection,
  forceChangesForDatasetChange
} from 'utils/accelerationUtils';
import ApiUtils from 'utils/apiUtils/apiUtils';

import Button from '../Buttons/Button';
import Message from '../Message';
import AccelerationBasic from './Basic/AccelerationBasic';
import AccelerationAdvanced from './Advanced/AccelerationAdvanced';

const SECTIONS = [AccelerationBasic, AccelerationAdvanced];

@Radium
export class AccelerationForm extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired,
    fields: PropTypes.object,
    error: PropTypes.object, // top-level
    errors: PropTypes.object, // specific fields (currently unused)
    dirty: PropTypes.bool,
    updateFormDirtyState: PropTypes.func,
    submitFailed: PropTypes.bool,
    values: PropTypes.object,
    initializeForm: PropTypes.func,

    putReflection: PropTypes.func.isRequired,
    postReflection: PropTypes.func.isRequired,
    deleteReflection: PropTypes.func.isRequired
  };

  static defaultProps = {
    errors: {},
    fields: {},
    acceleration: Immutable.Map()
  };

  static childContextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired,
    lostFieldsByReflection: PropTypes.object.isRequired
  };

  initialValues = null;
  lostFieldsByReflection = {};
  suggestions = {};

  getChildContext() {
    const {error} = this.props;
    let reflectionSaveErrors = new Immutable.Map();
    if (error && Immutable.Map.isMap(error.message) && error.message.get('code') === 'COMBINED_REFLECTION_SAVE_ERROR') {
      reflectionSaveErrors = error.message.get('details').get('reflectionSaveErrors');
    }
    return {reflectionSaveErrors, lostFieldsByReflection: this.lostFieldsByReflection};
  }

  constructor(props) {
    super(props);

    const mode = this.getMustBeInAdvancedMode() ? 'ADVANCED' : 'BASIC';

    this.state = {
      mode,
      waitingForRecommendations: false,
      saving: false
    };
  }

  unmounted = false;
  componentWillUnmount() {
    this.unmounted = true;
  }

  componentWillMount() {
    let { reflections, dataset } = this.props;
    dataset = this.props.dataset.toJS();

    const lostFieldsByReflection = {};
    const aggregationReflectionValues = [];
    const rawReflectionValues = [];
    for (const reflection of reflections.toList().toJS()) {
      const {lostFields, reflection: fixedReflection} = forceChangesForDatasetChange(reflection, dataset);
      if (lostFields) lostFieldsByReflection[fixedReflection.id] = lostFields;

      if (fixedReflection.type === 'RAW') {
        rawReflectionValues.push(createReflectionFormValues(fixedReflection));
      } else {
        aggregationReflectionValues.push(createReflectionFormValues(fixedReflection));
      }
    }
    this.lostFieldsByReflection = lostFieldsByReflection;

    const {rawReflections, aggregationReflections} = this.props.fields;

    if (this.state.mode === 'BASIC' && !aggregationReflectionValues.length) {
      this.fetchRecommendations();
    }

    const defaultToEnabled = false;

    // can't do this in mapStateToProps it puts redux-form in a render loop
    if (!aggregationReflectionValues.length) {
      const firstAgg = createReflectionFormValues({type: 'AGGREGATION', enabled: defaultToEnabled});
      aggregationReflectionValues.push(firstAgg);
    }
    if (!rawReflectionValues.length) {
      const firstRaw = createReflectionFormValues({type: 'RAW', enabled: defaultToEnabled});
      if (this.state.mode === 'BASIC') {
        firstRaw.displayFields = dataset.fields.map(({name}) => ({name}));
      }
      this.suggestions[firstRaw.id] = firstRaw;
      rawReflectionValues.push(firstRaw);
    }

    aggregationReflectionValues.forEach(v => aggregationReflections.addField(v));
    rawReflectionValues.forEach(v => rawReflections.addField(v));

    this.syncAdvancedToBasic(aggregationReflectionValues[0]);

    this.updateInitialValues();
  }

  fetchRecommendations() {
    this.setState({waitingForRecommendations: true});
    ApiUtils.fetch(`dataset/${encodeURIComponent(this.props.dataset.get('id'))}/reflection/recommendation`, {method: 'POST'}).then((response) => {
      return response.json().then(({data: reflections}) => {
        ReactDOM.unstable_batchedUpdates(() => {
          if (!this.state.waitingForRecommendations || this.unmounted) return;
          if (this.state.mode === 'ADVANCED' || !reflections.length || !reflections.some(r => r.type === 'AGGREGATION')) {
            // protect - ensure we get at least one agg or if user switched to advanced mode
            this.setState({ waitingForRecommendations: false });
            return;
          }

          const {aggregationReflections} = this.props.fields;
          aggregationReflections.forEach(() => aggregationReflections.removeField());

          for (const reflection of reflections) {
            if (reflection.type !== 'AGGREGATION') continue;
            // we want to disable recommendations
            reflection.enabled = false;
            const values = createReflectionFormValues(reflection);
            this.suggestions[values.id] = values;
            aggregationReflections.addField(values);
          }

          this.setState({ waitingForRecommendations: false });
          this.updateInitialValues();
        });
      });
    }).catch((error) => {
      if (this.unmounted) return;

      // quietly treat recommendation failures as "no recommendations"
      console.error(error);
      this.setState({waitingForRecommendations: false});
    });
  }

  skipRecommendations = () => {
    this.setState({waitingForRecommendations: false});
  }

  updateInitialValues() {
    // let the redux update run so that this.props.values gets updated
    setTimeout(() => {
      //this.props.initializeForm(this.initialValues, true);
      this.initialValues = {...this.props.values};
      this.props.updateFormDirtyState(false);
    }, 0);
  }

  componentWillReceiveProps(nextProps) {
    if (this.state.saving) {
      return;
    }

    const nextFirstAggValues = nextProps.values.aggregationReflections[0];
    const currFirstAggValues = this.props.values.aggregationReflections[0];
    if (!deepEqual(nextFirstAggValues, currFirstAggValues)) {
      this.syncAdvancedToBasic(nextFirstAggValues, nextProps);
    }

    if (
      !deepEqual(nextProps.values.columnsDimensions, this.props.values.columnsDimensions)
      || !deepEqual(nextProps.values.columnsMeasures, this.props.values.columnsMeasures)
    ) {
      this.syncBasicToAdvanced(nextProps);
    }

    if (this.state.mode === 'BASIC' && this.getMustBeInAdvancedMode(nextProps)) this.setState({mode: 'ADVANCED'});

    // manually calculate the dirty state depending on if values equals the initial values we computed in componentWillMount
    // TODO: not sure why an initializeForm call doesn't work property to reset the redux-form's state.  Sadly we
    // can't just call updateFormDirtyState in componentWillMount as redux-form thinks its dirty already and won't send
    // the set dirty calls even if more changes are made.
    this.props.updateFormDirtyState(!deepEqual(nextProps.values, this.initialValues));
  }

  syncAdvancedToBasic(firstAggValues, props = this.props) {
    const {columnsDimensions, columnsMeasures} = props.fields;

    columnsDimensions.forEach(() => columnsDimensions.removeField());
    columnsMeasures.forEach(() => columnsMeasures.removeField());

    firstAggValues.dimensionFields.forEach(
      ({name}) => columnsDimensions.addField({column: name})
    );
    firstAggValues.measureFields.forEach(
      ({name}) => columnsMeasures.addField({column: name})
    );
  }

  syncBasicToAdvanced(props = this.props) {
    const columnsDimensionsValues = props.values.columnsDimensions;
    const columnsMeasuresValues = props.values.columnsMeasures;
    const firstAgg = props.fields.aggregationReflections[0];

    // Note: we are careful to preserve granularity when syncing this direction

    const dimensionSet = new Set(columnsDimensionsValues.map(v => v.column));
    const measureSet = new Set(columnsMeasuresValues.map(v => v.column));

    firstAgg.dimensionFields.forEach(({name}, i) => {
      if (!dimensionSet.delete(name.value)) firstAgg.dimensionFields.removeField(i);
    });
    firstAgg.measureFields.forEach(({name}, i) => {
      if (!measureSet.delete(name.value)) firstAgg.measureFields.removeField(i);
    });

    for (const name of dimensionSet) firstAgg.dimensionFields.addField({name});
    for (const name of measureSet) firstAgg.measureFields.addField({name});
  }

  toggleMode = () => {
    const { mode } = this.state;
    this.setState({
      mode: mode === 'BASIC' ? 'ADVANCED' : 'BASIC'
    });
  }

  clearReflections = () => {
    this.props.fields.rawReflections.concat(this.props.fields.aggregationReflections).forEach(reflection => reflection.shouldDelete.onChange(true));
  }

  prepare(values) {
    const { mode } = this.state;

    let reflections = [...values.aggregationReflections, ...values.rawReflections];

    for (const reflection of reflections) {
      if (mode === 'BASIC' && reflection.type === 'RAW' && !reflection.enabled) {
        reflection.shouldDelete = true;
      }

      if (mode === 'BASIC' && reflection.type === 'AGGREGATION' && !reflection.enabled && areReflectionFormValuesUnconfigured(reflection)) {
        reflection.shouldDelete = true;
      }

      if (this.suggestions[reflection.id] && !reflection.enabled) {
        if (deepEqual(this.suggestions[reflection.id], reflection)) {
          reflection.shouldDelete = true;
        }
      }

      fixupReflection(reflection, this.props.dataset);
    }

    reflections = reflections.filter(reflection => {
      // can simply ignore new reflections which were then deleted
      if (!reflection.tag && reflection.shouldDelete) {
        return false;
      }

      return true;
    });

    // todo: reveal/scroll to errored reflection
    const errors = {};
    for (const reflection of reflections) {
      if (reflection.shouldDelete || !reflection.enabled) continue;

      if (reflection.type === 'RAW') {
        if (!reflection.displayFields.length) {
          errors[reflection.id] = la('At least one display field per raw Reflection is required.');
        }
      } else { // AGGREGATION
        if (!reflection.dimensionFields.length && !reflection.measureFields.length) { // eslint-disable-line no-lonely-if
          errors[reflection.id] = la('At least one dimension or measure field per aggregation Reflection is required.');
        }
      }
    }

    return {reflections, errors};
  }

  updateReflection(id, data) {
    const field = this.props.fields.rawReflections.concat(this.props.fields.aggregationReflections).find(reflection => reflection.id.value === id);
    if (field) {
      for (const [key, value] of Object.entries(data)) {
        field[key].onChange(value);
      }
    }
  }

  submitForm = (values) => {
    const {reflections, errors} = this.prepare(values);

    this.setState({saving: true});

    const promises = reflections.map(reflection => {
      const reflectionId = reflection.id;
      if (errors[reflectionId]) return; // don't save

      const shouldDelete = reflection.shouldDelete;
      delete reflection.shouldDelete;

      // add the datasetid before we send the reflections out
      reflection.datasetId = this.props.dataset.get('id');

      let promise;
      let cleanup;
      if (!reflection.tag) { // new, unsaved, reflections have fake ids for tracking, but no tag
        delete reflection.id;
        delete reflection.tag; // todo: need to do?

        promise = this.props.postReflection(reflection);
        cleanup = ({id, tag}) => {
          this.updateReflection(reflectionId, {tag, id}); // no longer new
        };
      } else if (shouldDelete) {
        promise = this.props.deleteReflection(reflection);
        cleanup = () => {
          this.updateReflection(reflectionId, { // it's now new
            tag: '',
            id: uuid.v4()
          });
        };
      } else {
        promise = this.props.putReflection(reflection); // todo: assign tag
        cleanup = ({id, tag}) => {
          this.updateReflection(reflectionId, {tag}); // tag may have updated
        };
      }

      // not using ApiUtils.attachFormSubmitHandlers because not yet ready to map validation errors to specific fields
      // Also: need to collect all the errors (not default Promise.all behavior), so catching everything without re-reject (until later)
      return promise.then((action) => {
        if (!action || !action.error) {
          const newData = action.payload && action.payload.get('entities').get('reflection').first().toJS();
          cleanup(newData); // make it so that if some reflection saving succeeds and some fail the user can correct issues and resubmit safely
          return action;
        }
        const error = action.payload;

        // todo: Charles abort not respected (modal closes) - why????
        // todo: if a delete succeeds and another call fails then we can end up with no reflections of a type

        // start with fallback
        errors[reflectionId] = error.message || la('Something went wrong.');

        const {response} = error;
        if (response) {
          if (response.errorMessage || response.message) {
            errors[reflectionId] = response;
          }
        }

        return action;
      }).catch((error) => {
        errors[reflectionId] = 'Request Error: ' + error.statusText; // todo: loc
      });
    });

    return Promise.all(promises).then((data) => {
      this.setState({saving: false});

      if (Object.keys(errors).length) return this.createSubmitErrorWrapper(errors, [...values.aggregationReflections, ...values.rawReflections].length);
    });
  }

  createSubmitErrorWrapper(reflectionSaveErrors, totalCount) {
    reflectionSaveErrors = Immutable.Map(reflectionSaveErrors).map((message, reflectionId) => Immutable.fromJS({
      id: uuid.v4(),
      message
    }));
    return Promise.reject({
      _error: {
        id: uuid.v4(),
        message: Immutable.fromJS({
          // no #message needed, code used
          code: 'COMBINED_REFLECTION_SAVE_ERROR',
          details: {
            reflectionSaveErrors,
            totalCount
          }
        })
      }
    });
  }

  renderHeader() {
    const { mode } = this.state;

    return (
      <div>
        <div style={{float: 'right'}}>
          {mode === 'ADVANCED' && <Button disableSubmit onClick={this.clearReflections} type='CUSTOM' text={la('Remove All Reflections')} />}
          <Button
            disable={mode === 'ADVANCED' && this.getMustBeInAdvancedMode()}
            disableSubmit
            onClick={this.toggleMode}
            type='CUSTOM'
            style={{ marginLeft: 10, width: 120 }} // lock width the prevent wiggle on toggle
            text={mode === 'BASIC' ? la('Switch to Advanced') : la('Revert to Basic')}
          />
        </div>
        <FormTitle>
          {la('Reflections')}
        </FormTitle>
      </div>
    );
  }

  getMustBeInAdvancedMode(props) {
    const {aggregationReflections, rawReflections} = props ? props.values : this.props.values;

    if (aggregationReflections.length > 1 || rawReflections.length > 1) {
      return true;
    }

    // out of sync is not in form data, so we check independently
    if (this.props.reflections.some(reflection => reflection.get('status').get('config') === 'INVALID')) {
      return true;
    }

    return [...aggregationReflections, ...rawReflections].some(reflection => !areReflectionFormValuesBasic(reflection, this.props.dataset.toJS()));
  }

  renderAccelerationMode() {
    const { fields, location, values, updateFormDirtyState, dataset, reflections } = this.props;
    const { mode, waitingForRecommendations } = this.state;

    if (mode === 'BASIC') {
      return (
        <AccelerationBasic
          dataset={dataset}
          reflections={reflections}
          location={location}
          fields={fields}
          loadingRecommendations={waitingForRecommendations}
          skipRecommendations={this.skipRecommendations}
        />
      );
    } else if (mode === 'ADVANCED') {
      return <AccelerationAdvanced
        dataset={dataset}
        reflections={reflections}
        fields={fields}
        values={values}
        updateFormDirtyState={updateFormDirtyState}/>;
    }
  }

  renderFormErrorMessage() {
    const { errors, submitFailed } = this.props;
    const listOfErrors = getListErrorsFromNestedReduxFormErrorEntity(errors);
    if (listOfErrors.length > 0 && submitFailed) {
      return (
        <div>
          { listOfErrors.map((errorMessage, i) =>
            <Message
              key={errorMessage}
              messageType='error'
              message={errorMessage}
              messageId={'' + i}
            />
          )}
        </div>
      );
    }
  }

  renderExtraErrorMessages() {
    const messages = [];

    const {layoutId} = (this.props.location.state || {});
    if (layoutId) {
      const found = this.props.reflections.get(layoutId);

      if (!found) {
        messages.push(<Message
          key='not-found'
          messageType='warning'
          message={Immutable.Map({code: 'REQUESTED_REFLECTION_MISSING'})}
          isDismissable={false}
          style={styles.extraError}
        />);
      }
    }

    if (this.props.reflections.some(reflection => reflection.get('status').get('config') === 'INVALID')) {
      messages.push(<Message
        key='version-mismatch'
        messageType='warning'
        message={Immutable.Map({code: 'COMBINED_REFLECTION_CONFIG_INVALID'})}
        isDismissable={false}
        style={styles.extraError}
      />);
    }

    return messages;
  }

  render() {
    const { handleSubmit, onCancel } = this.props;

    return (
      <div style={styles.base}>
        <ModalForm
          {...modalFormProps(this.props)}
          onSubmit={handleSubmit(this.submitForm)}
          onCancel={onCancel}
        >
          {this.renderFormErrorMessage()}
          {this.renderExtraErrorMessages()}
          <FormBody style={styles.formBody} dataQa='acceleration-form'>
            {this.renderHeader()}
            {this.renderAccelerationMode()}
          </FormBody>
        </ModalForm>
      </div>
    );
  }
}

const styles = {
  base: {
    height: '100%',
    display: 'flex',
    flexDirection: 'column'
  },
  formBody: {
    height: '100%',
    display: 'flex',
    flexDirection: 'column'
  },
  extraError: {
    marginBottom: 0
  }
};

function mapStateToProps(state, props) {
  return {
    location: state.routing.locationBeforeTransitions
  };
}

export default connectComplexForm({
  form: 'accelerationForm',
  skipDirtyFields: ['columnsDimensions', 'columnsMeasures']
}, SECTIONS, mapStateToProps, {
  putReflection: reflectionActions.put.dispatch,
  postReflection: reflectionActions.post.dispatch,
  deleteReflection: reflectionActions.delete.dispatch
})(AccelerationForm);
