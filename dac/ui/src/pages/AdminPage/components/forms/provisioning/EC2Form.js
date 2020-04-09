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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { merge } from 'lodash/object';

import { connectComplexForm } from '@app/components/Forms/connectComplexForm';
import { ModalForm, modalFormProps } from '@app/components/Forms';
import FormBody from '@app/components/Forms/FormBody';
import FormTab from '@app/components/Forms/FormTab';
import { applyValidators, isRequired } from '@app/utils/validation';
import FormTabConfig from '@app/utils/FormUtils/FormTabConfig';
import EC2FormMixin, { getInitValuesFromVlh } from 'dyn-load/pages/AdminPage/components/forms/provisioning/EC2FormMixin';
import {CLUSTER_STATE} from '@app/constants/provisioningPage/provisioningConstants';
import { EC2_FIELDS } from 'dyn-load/constants/provisioningPage/provisioningConstants';
import { loadAwsDefaults } from '@app/actions/resources/provisioning';
import { getAwsDefaults } from '@app/selectors/provision';
import {
  isEditMode,
  isRestartRequired,
  getInitValuesFromProvision,
  prepareProvisionValuesForSave
} from '@app/pages/AdminPage/components/forms/provisioning/provisioningFormUtil';

const FUNCTIONAL_ELEMENTS_EMPTY = [];

function getInitialValues(provision, awsDefaults) {
  const initValuesFromVlh = getInitValuesFromVlh();
  if (awsDefaults) {
    getInitValuesFromProvision(awsDefaults, initValuesFromVlh); //mutates 2nd arg
  }
  if (provision && provision.size) {
    const initValuesFromProvision = getInitValuesFromProvision(provision);
    merge(initValuesFromVlh, initValuesFromProvision); // mutates 1st argument
  }
  return initValuesFromVlh;
}

function validate(values) {
  let validators = {...applyValidators(values, [
    isRequired('name', la('Name')),
    isRequired('containerCount', la('Instance Count')),
    isRequired('sshKeyName', la('SSH Key Name')),
    isRequired('nodeIamInstanceProfile', la('IAM Role for S3 Access')),
    isRequired('securityGroupId', la('Security Group ID'))
  ])};
  if (values.authMode === 'SECRET') {
    validators = {
      ...validators,
      ...applyValidators(values, [
        isRequired('accessKey', la('Access Key')),
        isRequired('secretKey', la('Secret'))
      ])};
  }
  return validators;
}

@EC2FormMixin
export class EC2Form extends Component {

  static propTypes = {
    provision: PropTypes.object,
    onFormSubmit: PropTypes.func,
    onCancel: PropTypes.func,
    style: PropTypes.object,
    //connected
    fields: PropTypes.object.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    dirty: PropTypes.bool,
    loadAwsDefaults: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.config = new FormTabConfig(this.getVlh(), FUNCTIONAL_ELEMENTS_EMPTY);
  }

  config = null; //initialized in the constructor

  componentDidMount() {
    return this.props.loadAwsDefaults();
  }

  prepareValuesForSave = values => {
    const payload = prepareProvisionValuesForSave(values);
    // add props for edit mode
    const {provision} = this.props;
    if (isEditMode(provision)) {
      payload.id = provision.get('id');
      payload.tag = provision.get('tag');
      payload.desiredState = CLUSTER_STATE.running;
    }
    return payload;
  };

  submit = values => {
    const {provision, dirty} = this.props;
    return this.props.onFormSubmit(this.prepareValuesForSave(values), isRestartRequired(provision, dirty));
  };

  render() {
    const {fields, handleSubmit, onCancel, style, provision} = this.props;
    const btnText = (isEditMode(provision)) ? la('Save') : la('Save & Launch');
    return (
      <ModalForm
        {...(modalFormProps(this.props) || {})}
        onSubmit={handleSubmit(this.submit)}
        onCancel={onCancel}
        confirmText={btnText}
      >
        <FormBody style={style}>
          <FormTab fields={fields} tabConfig={this.config}/>
        </FormBody>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, props) {
  const awsDefaults = getAwsDefaults(state);
  const initialValues = getInitialValues(props.provision, awsDefaults);
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'EC2',
  fields: EC2_FIELDS,
  validate,
  initialValues: getInitialValues()
}, [], mapStateToProps, {loadAwsDefaults})(EC2Form);
