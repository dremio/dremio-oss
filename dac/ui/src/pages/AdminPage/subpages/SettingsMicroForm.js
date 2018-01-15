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
import { PureComponent } from 'react';
import { connect } from 'react-redux';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { propTypes as reduxFormPropTypes} from 'redux-form';

import { InnerComplexForm, connectComplexForm } from 'components/Forms/connectComplexForm.js';
import SimpleButton from 'components/Buttons/SimpleButton';
import TextField from 'components/Fields/TextField';
import Toggle from 'components/Fields/Toggle';
import FieldWithError from 'components/Fields/FieldWithError';
import { isRequired, isNumber, isInteger } from 'utils/validation';
import ApiUtils from 'utils/apiUtils/apiUtils';
import settingActions from 'actions/resources/setting';

import {LABELS, FIELD_OVERRIDES} from './settingsConfig';

@Radium
export class SettingsMicroForm extends PureComponent {
  static propTypes = {
    ...reduxFormPropTypes,

    viewId: PropTypes.string.isRequired,

    settingId: PropTypes.string.isRequired,
    setting: PropTypes.instanceOf(Immutable.Map),

    putSetting: PropTypes.func.isRequired,
    getSetting: PropTypes.func.isRequired,

    fields: PropTypes.object.isRequired,

    showLabel: PropTypes.bool.isRequired,

    style: PropTypes.object
  }

  static defaultProps = {
    showLabel: true
  }

  submit = (form) => {
    if (!this.props.fields.value.dirty) {
      return Promise.resolve(); // early enter/return reject
    }

    const data = {
      ...this.props.setting.toJS(),
      ...form,
      showOutsideWhitelist: true
    };

    const type = this.props.setting.get('type');
    if (type === 'INTEGER' || type === 'FLOAT') {
      data.value = +data.value;
    }

    return ApiUtils.attachFormSubmitHandlers(
      this.props.putSetting(data, {viewId: this.props.viewId})
    ).then(() => this.props.getSetting(this.props.settingId, {viewId: this.props.viewId}));
  }

  renderField() {
    const OverrideField = FIELD_OVERRIDES[this.props.settingId];
    if (OverrideField) {
      return <OverrideField {...this.props.fields.value} />;
    }

    switch (this.props.setting.get('type')) {
    case 'BOOLEAN':
      return <div style={{display: 'inline-block', marginTop: 2, marginLeft: 3, verticalAlign: '-0.8em'}}>
        <Toggle {...this.props.fields.value} />
      </div>;
    case 'INTEGER':
    case 'FLOAT': // todo: create dedicated int and number inputs
      return <TextField {...this.props.fields.value} type='number' />;
    case 'TEXT':
    default:
      return <TextField {...this.props.fields.value} />;
    }
  }

  render() {
    const {setting, showLabel} = this.props;
    if (!setting) {
      return null;
    }
    const id = setting.get('id');

    const buttonStyle = {
      verticalAlign: 0,
      // use visibility so that it takes up space (e.g. in Support table)
      visibility: this.props.fields.value.dirty ? 'visible' : 'hidden',
      minWidth: 50
    };

    let label = null;
    if (showLabel && LABELS[id] !== '') { // todo: ax
      label = <b style={{display: 'block'}}>{LABELS[id] || id}:</b>;
    }
    return <InnerComplexForm {...this.props} onSubmit={this.submit} style={{display: 'block'}}>
      <FieldWithError errorPlacement='right' {...this.props.fields.value}>
        <div style={{...this.props.style, display: 'inline-block', paddingRight: 20}}>
          <label>
            {label}
            {this.renderField()}
          </label>
          {/* todo: by default buttons and textfields and toggles don't align. need to (carefully) fix */}
          <SimpleButton buttonStyle='secondary' style={buttonStyle}>
            {la('Save')}
          </SimpleButton>
        </div>
      </FieldWithError>
    </InnerComplexForm>;
  }
}

function mapToFormState(state, ownProps) {
  return {
    initialValues: {
      ...ownProps.initialValues,
      value: ownProps.setting && ownProps.setting.get('value')
    }
  };
}

const ConnectedForm = connectComplexForm({
  fields: ['value'],
  validate: (values, props) => {
    if (!props.setting) {
      return;
    }

    let errors = {
      ...isRequired('value', 'Value')(values)
    };

    const type = props.setting.get('type');
    if (type === 'INTEGER') {
      errors = {...errors, ...isInteger('value', 'Value')(values)};
    } else if (type === 'FLOAT') {
      errors = {...errors, ...isNumber('value', 'Value')(values)};
    }

    return errors;
  }
}, [], mapToFormState, {
  putSetting: settingActions.put.dispatch,
  getSetting: settingActions.get.dispatch
})(SettingsMicroForm);

// This little guard makes sure that the form doesn't initialize with the wrong value type with the data is loading in
export default connect((state, ownProps) => {
  const setting = state.resources.entities.getIn(['setting', ownProps.settingId]);
  return {setting};
})((props) => {
  if (!props.setting) return null;
  return <ConnectedForm {...props} />;
});
