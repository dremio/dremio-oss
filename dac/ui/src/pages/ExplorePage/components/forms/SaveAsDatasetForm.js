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

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { FieldWithError, TextField } from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';
import { getInitialResourceLocation, constructFullPath } from 'utils/pathUtils';
import ResourceTreeController from 'components/Tree/ResourceTreeController';
import DependantDatasetsWarning from 'components/Modals/components/DependantDatasetsWarning';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import Message from 'components/Message';
import { formRow, label } from 'uiTheme/radium/forms';

export const FIELDS = ['name', 'location', 'reapply'];

function validate(values) {
  return applyValidators(values, [
    isRequired('name'),
    isRequired('location')]);
}

const locationType = PropTypes.string;
// I would like to enforce that initial value and field value for location has the same type,
// as inconsistency in these 2 parameters, cause redux-form treat a form as dirty.
// I created this as function, not as standalone object, to avoid eslint errors that requires to
// document the rest of redux-form properties: onChange, error, touched
const getLocationPropType = () => PropTypes.shape({
  value: locationType
});

export class SaveAsDatasetForm extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    message: PropTypes.string,
    canReapply: PropTypes.bool,
    datasetType: PropTypes.string,
    handleSubmit: PropTypes.func.isRequired,
    dependentDatasets: PropTypes.array,
    updateFormDirtyState: PropTypes.func,

    // redux-form
    initialValues: PropTypes.shape({
      location: locationType
    }),
    fields: PropTypes.shape({
      location: getLocationPropType()
    })
  };

  static contextTypes = {
    location: PropTypes.object
  }

  handleChangeSelectedNode = (nodeId, node) => {
    this.props.fields.location.onChange(node && constructFullPath(node.get('fullPath').toJS()) || nodeId);
  };

  renderWarning() {
    const { dependentDatasets } = this.props;

    if (dependentDatasets && dependentDatasets.length > 0) {
      return (
        <DependantDatasetsWarning
          text={`Changing the name of this dataset
              will disconnect ${dependentDatasets.length} dependent
              datasets. Make a copy to preserve these connections.`}
          dependantDatasets={dependentDatasets}
        />
      );
    }

    return null;
  }

  renderHistoryWarning() {
    const { version, tipVersion } = this.context.location.query;
    if (tipVersion && tipVersion !== version) {
      return (
        <DependantDatasetsWarning
          text={la('You may lose your previous changes.')}
          dependantDatasets={[]}
        />
      );
    }

    return null;
  }

  render() {
    const { fields: { name, location }, handleSubmit, onFormSubmit, message } = this.props;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        {this.renderWarning()}
        {this.renderHistoryWarning()}
        <FormBody>
          { message && <div style={formRow}>{message}</div>}
          <div style={formRow}>
            <FieldWithError label='Name' {...name}>
              <TextField initialFocus {...name}/>
            </FieldWithError>
          </div>
          <div style={formRow}>
            <label style={label}>Location</label>
            <ResourceTreeController
              isDatasetsDisabled
              hideSources
              onChange={this.handleChangeSelectedNode}
              preselectedNodeId={location.initialValue}
              showFolders/>
            {
              this.props.fields.location.error && this.props.fields.location.touched &&
                <Message messageType='error' message={this.props.fields.location.error} />
            }
          </div>
        </FormBody>
      </ModalForm>
    );
  }
}

const mapStateToProps = (state, props) => ({
  initialValues: {
    location: getInitialResourceLocation(props.fullPath, props.datasetType, state.account.getIn(['user', 'userName']))
  }
});

export default connectComplexForm({
  form: 'saveAsDataset',
  fields: FIELDS,
  validate
}, [], mapStateToProps, null)(SaveAsDatasetForm);
