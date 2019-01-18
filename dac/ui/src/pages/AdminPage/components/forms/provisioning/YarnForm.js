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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import HOCON from 'hoconfig-js/lib/parser';

import { applyValidators, isNumber, isRequired } from 'utils/validation';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { PROVISION_MANAGERS } from 'constants/provisioningPage/provisionManagers';
import * as PROVISION_DISTRIBUTIONS from 'constants/provisioningPage/provisionDistributions';
import { FormBody, ModalForm, modalFormProps } from 'components/Forms';
import NumberFormatUtils from 'utils/numberFormatUtils';
import YarnProperties from 'components/Forms/YarnProperties';
import { Checkbox, FieldWithError, Select, TextField } from 'components/Fields';
import { formRow, label, sectionTitle } from 'uiTheme/radium/forms';
import { formDefault, formLabel } from 'uiTheme/radium/typography';
import TextFieldList from 'components/Forms/TextFieldList';
import { formatMessage } from 'utils/locale';
import config from 'utils/config';
import { inputSpacing as inputSpacingCssValue } from '@app/uiTheme/less/variables.less';

const FIELDS = [
  'id', 'clusterType', 'resourceManagerHost', 'namenodeHost', 'queue',
  'memoryMB', 'virtualCoreCount', 'dynamicConfig.containerCount',
  'propertyList[].name', // we map from entity.key -> field.name in mapToFormFields to match what Property input expects.
  'propertyList[].value', 'propertyList[].type', 'tag', 'distroType', 'isSecure',
  'spillDirectories[]'
];

const DEFAULT_MEMORY = 16;
const DEFAULT_CORES = 4;

function getMinErrors(values) {
  const errors = {};
  if (config.lowerProvisioningSettingsEnabled) return errors;

  if (values.memoryMB < DEFAULT_MEMORY) {
    errors.memoryMB = formatMessage('Yarn.MinMemoryError', { default: DEFAULT_MEMORY });
  }
  if (values.virtualCoreCount < DEFAULT_CORES) {
    errors.virtualCoreCount = formatMessage('Yarn.MinCoresError', { default: DEFAULT_CORES });
  }
  return errors;
}

function validate(values) {
  return {
    ...getMinErrors(values),
    ...applyValidators(values, [
      isRequired('resourceManagerHost', la('Resource Manager')),
      isRequired('namenodeHost', YarnForm.hostNameLabel(values)),
      isRequired('virtualCoreCount', la('Cores per Worker')),
      isRequired('memoryMB', la('Memory per Worker')),
      isRequired('dynamicConfig.containerCount', la('Workers')),
      isNumber('virtualCoreCount', la('Cores per Worker')),
      isNumber('memoryMB', la('Memory per Worker')),
      isNumber('dynamicConfig.containerCount', la('Workers'))
    ]),
    ...applyValidators(values, values.spillDirectories.map((item, index) => {
      return isRequired(`spillDirectories.${index}`, la('Spill Directory'));
    }))
  };
}

@Radium
export class YarnForm extends Component {

  static propTypes = {
    onCancel: PropTypes.func.isRequired,
    onFormSubmit: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    fields: PropTypes.object,
    provision: PropTypes.instanceOf(Immutable.Map),
    values: PropTypes.object,
    dirty: PropTypes.bool,
    style: PropTypes.object
  };

  /**
   * Generate form fields from provision entity
   */
  static mapToFormFields(provision) {
    const fields = {
      ...provision.toJS()
    };
    const cluster = PROVISION_MANAGERS.find(manager => manager.clusterType === provision.get('clusterType'));
    const propertyList = provision.get('subPropertyList').filter((property) => {
      return !cluster.propsAsFields.some((propAsField) => propAsField.key === property.get('key'));
    }).map((property) => ({
      name: property.get('key'),
      value: property.get('value'),
      type: property.get('type')
    })).toJS();

    cluster.propsAsFields.forEach((propAsField) => {
      fields[propAsField.field] = provision.get('subPropertyList')
        .find((property) => propAsField.key === property.get('key')).get('value');
      if (fields.spillDirectories) {
        try {
          fields.spillDirectories = HOCON.parse('value:' + fields.spillDirectories).value;
          if (!Array.isArray(fields.spillDirectories)) {
            throw new Error('spillDirectories was not an array');
          }
        } catch (error) {
          console.error(error);
          // someone forced an invalid HOCON array into the system! This should be impossible via the FE, but
          // since this value is just a string for most of its life it isn't validated as it normally would.
          // (This could happen to someone using the API directly.)
          // For now, just reset the value to `['']` so that the user has to re-enter
          fields.spillDirectories = [''];
        }
      }
    });

    return {
      ...fields,
      // we show value in GB
      memoryMB: NumberFormatUtils.roundNumberField(fields.memoryMB / 1024),
      propertyList
    };
  }

  static distributionDirectory(distribution) {
    const { MAPR } = PROVISION_DISTRIBUTIONS;
    const defaultDirectory = 'file:///var/log/dremio';
    return {
      [MAPR]: 'maprfs:///var/mapr/local/${NM_HOST}/mapred/spill'
    }[distribution] || defaultDirectory;
  }

  static hostNameLabel(values) {
    const defaultLabel = la('NameNode');
    const { MAPR } = PROVISION_DISTRIBUTIONS;
    const { distroType } = values;
    const hostNameLabels = {
      [MAPR]: la('CLDB')
    };
    return hostNameLabels[distroType] || defaultLabel;
  }

  static hostNamePrefix(distribution) {
    const defaultPrefix = '';
    const { MAPR } = PROVISION_DISTRIBUTIONS;
    return {
      [MAPR]: 'maprfs:///'
    }[distribution] || defaultPrefix;
  }

  componentWillReceiveProps(nextProps) {
    const oldDistroType = this.props.values.distroType;
    const newDistroType = nextProps.values.distroType;
    const distroChanged = oldDistroType !== newDistroType;
    const nextValues = nextProps.values;
    if (distroChanged) {
      // update spill directory if user didn't change its value
      if (YarnForm.distributionDirectory(oldDistroType) === nextValues.spillDirectories[0]) {
        nextProps.fields.spillDirectories[0].onChange(YarnForm.distributionDirectory(newDistroType));
      }
      // update host name prefix if user didn't change its value
      if (YarnForm.hostNamePrefix(oldDistroType) === nextValues.namenodeHost) {
        nextProps.fields.namenodeHost.onChange(YarnForm.hostNamePrefix(newDistroType));
      }
    }
  }

  /**
   * Generate values used on submit from form fields
   */
  static normalizeValues(values) {
    const result = Object.keys(values).reduce((fields, fieldName) => {
      const value = values[fieldName];
      let field = {[fieldName]: value};
      if (fieldName === 'memoryMB') {
        field[fieldName] = value * 1024;
      } else if (fieldName === 'resourceManagerHost') {
        field = {key: 'yarn.resourcemanager.hostname', value};
        fields.subPropertyList.push(field);
        return fields;
      } else if (fieldName === 'namenodeHost') {
        field = {key: 'fs.defaultFS', value};
        fields.subPropertyList.push(field);
        return fields;
      } else if (fieldName === 'spillDirectories') {
        field = {key: 'paths.spilling', value: JSON.stringify(value)}; // JSON is compatible with hocon in this scenario
        fields.subPropertyList.push(field);
        return fields;
      }
      return {
        ...fields,
        ...field
      };
    }, {subPropertyList: []});

    result.subPropertyList = values.propertyList.map(
      (property) => ({key: property.name, value: property.value, type: property.type})
    ).concat(result.subPropertyList || []);
    delete result.propertyList;
    return result;
  }

  getDistributionOptions() {
    const { MAPR, APACHE, HDP, CDH, OTHER } = PROVISION_DISTRIBUTIONS;
    return [
      {option: APACHE, label :la('Apache')},
      {option: CDH, label: la('Cloudera')},
      {option: HDP, label: la('Hortonworks')},
      {option: MAPR, label: la('MapR')},
      {option: OTHER, label: la('Other')}
    ];
  }

  isEditMode() {
    return !!this.props.provision.size;
  }

  getIsRestartRequired() { // when we have more sources this will need to be abstracted out, but leaving here for now
    const isEditMode = this.isEditMode();
    const currentState = this.props.provision.get('currentState');
    const isRunning = currentState === 'RUNNING';
    if (!isEditMode || !isRunning) return false;
    return Object.entries(this.props.fields).some(([key, field]) => {
      if (key === 'dynamicConfig') return false;
      if (key === 'propertyList') {
        return field.some(item => item.name.dirty || item.value.dirty);
      }
      if (key === 'spillDirectories') {
        return field.some(item => item.dirty);
      }
      return field.dirty;
    });
  }

  submitForm = (values) => {
    return this.props.onFormSubmit(YarnForm.normalizeValues(values), this.getIsRestartRequired());
  }

  render() {
    const { fields, handleSubmit, style } = this.props;

    const confirmText = this.getIsRestartRequired() ? la('Restart') : la('Save');
    const hostNameLabel = YarnForm.hostNameLabel(this.props.values);

    return (
      <ModalForm
        {...modalFormProps(this.props)}
        onSubmit={handleSubmit(this.submitForm)}
        confirmText={confirmText}>
        <FormBody style={style}>
          <h2 style={sectionTitle}>{la('General')}</h2>
          <div style={styles.formRow}>
            <div style={{display: 'inline-flex', marginRight: inputSpacingCssValue}}>
              <div style={styles.inlineBlock}>
                <div style={label}>{la('Hadoop Cluster')}</div>
                <Select
                  name='distroType'
                  items={this.getDistributionOptions()}
                  disabled={this.isEditMode()}
                  {...fields.distroType}
                />
              </div>
            </div>
            <Checkbox
              style={{paddingTop: label.fontSize}}
              label={la('This is a secure cluster')}
              disabled={this.isEditMode()}
              {...fields.isSecure}
            />
          </div>
          <div style={styles.formRow}>
            <FieldWithError
              style={styles.inlineBlock}
              labelStyle={formLabel}
              label={la('Resource Manager')}
              errorPlacement='top'
              {...fields.resourceManagerHost}>
              <TextField initialFocus {...fields.resourceManagerHost}/>
            </FieldWithError>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={hostNameLabel}
              errorPlacement='top'
              {...fields.namenodeHost}>
              <TextField {...fields.namenodeHost}/>
            </FieldWithError>
          </div>
          <div style={styles.formRow}>
            <TextFieldList
              label={la('Spill Directories')}
              arrayField={fields.spillDirectories}
              addButtonText={la('Add Directory')}
              minItems={1} />
          </div>
          <div style={styles.formRow}>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={la('Queue')}
              errorPlacement='top'
              {...fields.queue}>
              <TextField {...fields.queue}/>
            </FieldWithError>
          </div>
          <div style={styles.formRow}>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={la('Workers')}
              errorPlacement='bottom'
              {...fields.dynamicConfig.containerCount}>
              <TextField {...fields.dynamicConfig.containerCount} style={{width: 75}}/>
            </FieldWithError>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={la('Cores per Worker')}
              errorPlacement='top'
              {...fields.virtualCoreCount}>
              <TextField {...fields.virtualCoreCount} style={{width: 100}}/>
            </FieldWithError>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={la('Memory per Worker')}
              errorPlacement='bottom'
              {...fields.memoryMB}>
              <span>
                <TextField {...fields.memoryMB} style={{width: 75}}/>
                <span style={formDefault}>{'GB'}</span>
              </span>
            </FieldWithError>

          </div>
          <div style={styles.formRow}>
            <FieldWithError {...fields.propertyList}>
              <YarnProperties
                title={la('Additional Properties')}
                emptyLabel={la('(No Options Added)')}
                addLabel={la('Add Option')}
                fields={fields}/>
            </FieldWithError>
          </div>
        </FormBody>
      </ModalForm>
    );
  }
}

function mapToFormState(state, props) {
  const { provision } = props;
  const initialValues = {
    ...props.initialValues,
    clusterType: 'YARN'
  };
  if (provision.size) {
    return {
      initialValues: {
        ...props.initialValues,
        ...YarnForm.mapToFormFields(provision)
      }
    };
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'YarnForm',
  validate,
  fields: FIELDS,
  initialValues: {
    spillDirectories: [YarnForm.distributionDirectory(PROVISION_DISTRIBUTIONS.APACHE)],
    namenodeHost: YarnForm.hostNamePrefix(PROVISION_DISTRIBUTIONS.APACHE),
    distroType: PROVISION_DISTRIBUTIONS.APACHE,
    isSecure: false,
    memoryMB: DEFAULT_MEMORY,
    virtualCoreCount: DEFAULT_CORES
  }
}, [], mapToFormState, null)(YarnForm);


const styles = {
  formRow: {
    ...formRow,
    display: 'flex'
  },
  inlineBlock: {
    display: 'inline-block'
  }
};
