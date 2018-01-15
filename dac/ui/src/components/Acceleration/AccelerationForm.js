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
import { Component } from 'react';
import { ModalForm, modalFormProps, FormBody, FormTitle, connectComplexForm } from 'components/Forms';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import uuid from 'uuid';

import { showConfirmationDialog, showClearReflectionsDialog } from 'actions/confirmation';
import { getListErrorsFromNestedReduxFormErrorEntity } from 'utils/validation';
import Button from '../Buttons/Button';
import Message from '../Message';
import AccelerationBasic from './Basic/AccelerationBasic';
import AccelerationAdvanced from './Advanced/AccelerationAdvanced';

const SECTIONS = [AccelerationBasic, AccelerationAdvanced];

@Radium
export class AccelerationForm extends Component {
  static propTypes = {
    acceleration: PropTypes.instanceOf(Immutable.Map).isRequired,
    fullPath: PropTypes.string,
    submit: PropTypes.func,
    deleteAcceleration: PropTypes.func,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired,
    fields: PropTypes.object,
    errors: PropTypes.object,
    dirty: PropTypes.bool,
    updateFormDirtyState: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    showClearReflectionsDialog: PropTypes.func,
    submitFailed: PropTypes.bool,
    values: PropTypes.object
  };

  static defaultProps = {
    errors: {},
    fields: {},
    acceleration: Immutable.Map()
  };


  state = {
    // acceleration mode is actually part of the acceleration form but we store and use it from state
    // because changing of `mode` value via redux form like mode.onChange('AUTO') will toggle form's
    // dirty state. This is not desired behavior here, since we don't want to show unsaved changed modal
    // when `mode` value is changed.
    mode: this.props.acceleration.get('mode')
  }

  onSwitchToAdvanced = () => {
    const { dirty } = this.props;
    if (dirty) {
      this.props.showConfirmationDialog({
        title: la('Unsaved Changes'),
        confirmText: la('Continue'),
        text: [
          la('Switching to advanced mode will cause you to lose unsaved changes to this acceleration.'),
          la('Are you sure you want to switch to advanced?')
        ],
        confirm: this.toggleMode
      });
    } else {
      this.toggleMode();
    }
  }

  toggleMode = () => {
    const { mode } = this.state;
    if (mode) {
      this.setState({
        mode: mode === 'AUTO' ? 'MANUAL' : 'AUTO'
      });
    }
  }

  deleteAcceleration = () => {
    const { acceleration, deleteAcceleration, onCancel } = this.props;
    this.props.updateFormDirtyState(false);
    return deleteAcceleration(acceleration.getIn(['id', 'id'])).then(() => onCancel());
  }

  clearReflections = () => {
    this.props.showClearReflectionsDialog({
      confirm: this.deleteAcceleration
    });
  }

  canSubmit() {
    const { acceleration } = this.props;
    return acceleration && acceleration.get('state') !== 'NEW';
  }

  submitForm = (values) => {
    const { acceleration } = this.props;

    cleanEmptyIds(values.aggregationLayouts.layoutList);
    cleanEmptyIds(values.rawLayouts.layoutList);

    const finalValues = {
      mode: this.state.mode,
      state: acceleration.get('state'),
      id: acceleration.get('id').toJS(),
      type: acceleration.get('type'),
      version: acceleration.get('version'),
      aggregationLayouts: values.aggregationLayouts,
      rawLayouts: values.rawLayouts,
      context: {
        dataset: acceleration.getIn(['context', 'dataset']).toJS(),
        datasetSchema: acceleration.getIn(['context', 'datasetSchema']).toJS(),
        logicalAggregation: {
          dimensionList: values.columnsDimensions.map((column) => ({name: column.column})),
          measureList: values.columnsMeasures.map((column) => ({name: column.column}))
        }
      }
    };

    if (finalValues.rawLayouts.enabled && finalValues.mode === 'MANUAL') {
      const { layoutList } = finalValues.rawLayouts;
      if (!layoutList.every(layout => layout.details.displayFieldList.length > 0)) {
        return Promise.reject({_error: {
          message: la('At least one display field per raw Reflection is required.'),
          id: uuid.v4()
        }});
      }
    }

    if (finalValues.aggregationLayouts.enabled) {
      if (finalValues.mode === 'AUTO' && (!values.columnsDimensions.length || !values.columnsMeasures.length)) {
        return Promise.reject({_error: {
          message: la('At least one dimension and measure field is required.'),
          id: uuid.v4()
        }});
      } else if (finalValues.mode === 'MANUAL' && !finalValues.aggregationLayouts.layoutList.every(layoutHasBoth)) {
        return Promise.reject({_error: {
          message: la('At least one dimension and measure field per aggregation Reflection is required.'),
          id: uuid.v4()
        }});
      }
    }

    return this.props.submit(finalValues);
  }

  renderHeader() {
    const { mode } = this.state;
    const currentToggleModeButton = mode === 'AUTO'
      ? <Button
        disableSubmit
        onClick={this.onSwitchToAdvanced}
        type='CUSTOM'
        style={{ marginLeft: 10 }}
        text={la('Switch to Advanced')}
      />
      : null;
      //<span onClick={this.toggleMode}>{la('Revert to Basic')}</span>;

    return (
      <div>
        <div style={{float: 'right'}}>
          <Button disableSubmit onClick={this.clearReflections} type='CUSTOM' text={la('Clear All Reflections')} />
          {currentToggleModeButton}
        </div>
        <FormTitle>
          {la('Reflections')}
        </FormTitle>
      </div>
    );
  }

  renderAccelerationMode() {
    const { acceleration, fields, location, fullPath, values, updateFormDirtyState } = this.props;
    const { mode } = this.state;

    if (!mode) {
      return null;
    }

    if (mode === 'AUTO') {
      return (
        <AccelerationBasic
          acceleration={acceleration}
          location={location}
          fullPath={fullPath}
          fields={this.props.fields}
        />
      );
    } else if (mode === 'MANUAL') {
      return <AccelerationAdvanced
        acceleration={acceleration}
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
            )
          }
        </div>
      );
    }
  }

  renderExtraErrorMessages() {
    const { acceleration } = this.props;

    const messages = [];

    const errorList = acceleration.get('errorList');
    if (errorList) {
      messages.push(...errorList.map((error, i) => <Message
        key={i}
        messageType='error'
        message={error}
        isDismissable={false}
        style={styles.extraError}
      />).toJS());
    }

    const {layoutId} = (this.props.location.state || {});
    if (layoutId) {
      const found = this.props.acceleration.getIn([
        'aggregationLayouts', 'layoutList'
      ]).concat(this.props.acceleration.getIn([
        'rawLayouts', 'layoutList'
      ])).some(layout => layout.get('id') === layoutId);

      if (!found) {
        messages.push(<Message
          key='not-found'
          messageType='warning'
          message={la('The requested Reflection no longer exists.')}
          isDismissable={false}
          style={styles.extraError}
        />);
      }
    }

    if (acceleration.get('state') === 'OUT_OF_DATE') {
      messages.push(<Message
        key='out-of-date'
        messageType='warning'
        message={la('This Dataset has been updated since Reflections were defined. ' +
                  'To use new or modified fields, you will need to first “Clear All Reflections”.')}
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
          canSubmit={this.canSubmit()}
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

function mapApiAggregationColumnsToForm(apiColumns, skipMeasure) {
  if (!apiColumns) {
    return [];
  }
  return apiColumns.map((column) => {
    const field = {
      column: column.get('name')
    };
    if (!skipMeasure) {
      field.measure = column.get('type');
    }
    return field;
  }).toJS();
}

function mapStateToProps(state, props) {
  const { acceleration } = props;
  const location = state.routing.locationBeforeTransitions;
  const columnsDimensions = mapApiAggregationColumnsToForm(
    acceleration.getIn(['context', 'logicalAggregation', 'dimensionList']), true
  );
  const columnsMeasures = mapApiAggregationColumnsToForm(
    acceleration.getIn(['context', 'logicalAggregation', 'measureList'])
  );
  return {
    location,
    initialValues: {
      state: acceleration.get('state'),
      aggregationLayouts: acceleration.get('aggregationLayouts').toJS(),
      rawLayouts: acceleration.get('rawLayouts').toJS(),
      columnsDimensions,
      columnsMeasures
    }
  };
}

export default connectComplexForm({
  form: 'accelerationForm',
  fields: [
    'rawLayouts.enabled'
  ]
}, SECTIONS, mapStateToProps, { showConfirmationDialog, showClearReflectionsDialog })(AccelerationForm);

const layoutHasBoth = layout => {
  return layout.details.dimensionFieldList.length > 0 && layout.details.measureFieldList.length > 0;
};

const cleanEmptyIds = layouts => {
  for (const layout of layouts) {
    if (!layout.id) delete layout.id;
  }
};
