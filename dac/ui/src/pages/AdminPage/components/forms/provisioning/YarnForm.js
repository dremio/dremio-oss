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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import HOCON from 'hoconfig-js/lib/parser';

import {applyValidators, isNumber, isRequired, noSpaces} from '@app/utils/validation';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import * as PROVISION_DISTRIBUTIONS from '@app/constants/provisioningPage/provisionDistributions';
import { FormBody, ModalForm, modalFormProps } from 'components/Forms';
import NumberFormatUtils from 'utils/numberFormatUtils';
import YarnProperties from 'components/Forms/YarnProperties';
import { Checkbox, FieldWithError, Select, TextField } from 'components/Fields';
import { formRow, label, sectionTitle } from 'uiTheme/radium/forms';
import { formDefault, formLabel } from 'uiTheme/radium/typography';
import TextFieldList from 'components/Forms/TextFieldList';
import { formatMessage } from 'utils/locale';
import { inputSpacing as inputSpacingCssValue } from '@app/uiTheme/less/variables.less';
import { isEditMode, isRestartRequired } from '@app/pages/AdminPage/components/forms/provisioning/provisioningFormUtil';

import config from 'dyn-load/utils/config';
import {
  FIELDS,
  INIT_VALUES,
  MAPPED_FIELDS,
  PROVISION_MANAGERS
} from 'dyn-load/constants/provisioningPage/provisionManagers';
import YarnFormMixin, { cacheValidators } from 'dyn-load/pages/AdminPage/components/forms/provisioning/YarnFormMixin';

const DEFAULT_MEMORY = 16;
const DEFAULT_CORES = 4;
const DEFAULT_CLUSTER_TYPE = 'YARN';

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
      isRequired(MAPPED_FIELDS.nodeTag, la('Engine Name')),
      isRequired(MAPPED_FIELDS.resourceManagerHost, la('Resource Manager')),
      isRequired(MAPPED_FIELDS.namenodeHost, YarnForm.hostNameLabel(values)),
      isRequired('virtualCoreCount', la('Cores per Worker')),
      isRequired('memoryMB', la('Memory per Worker')),
      isRequired('dynamicConfig.containerCount', la('Workers')),
      isNumber('virtualCoreCount', la('Cores per Worker')),
      isNumber('memoryMB', la('Memory per Worker')),
      isNumber('dynamicConfig.containerCount', la('Workers')),
      noSpaces(MAPPED_FIELDS.nodeTag, la('Engine Name'))
    ]),
    ...applyValidators(values, values.spillDirectories.map((item, index) => {
      return isRequired(`${MAPPED_FIELDS.spillDirectories}.${index}`, la('Spill Directory'));
    })),
    ...cacheValidators(values)
  };
}

@Radium
@YarnFormMixin
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

  static getPropsAsFields = (clusterType = DEFAULT_CLUSTER_TYPE) => {
    const cluster = PROVISION_MANAGERS.find(
      manager => manager.clusterType === clusterType
    );
    return cluster ? cluster.propsAsFields : [];
  };

  static addToPropertyList = (propertyList, key, value, type) => {
    //mutates propertyList
    propertyList.push({name: key, value, type});
  };

  /**
   * Generate form field values from provision entity
   * Entity has own simple props: [id, tag, name, clusterType, currentState, desiredState],
   *   dynamicConfig: {containerCount}, and
   *   yarnProps: [memoryMB, distroType, virtualCoreCount, isSecure, and nested subPropertyList: [{key,value,type}]]
   * We need to flatten yarnProps and convert subPropertyList into fields, vetting array sub-list entries
   */
  static getInitValuesFromProvision(provision) {
    const provisionObj = provision.toJS();
    // find sub props to fields config in PROVISION_MANAGERS for the current provision cluster type
    const propsAsFields = YarnForm.getPropsAsFields(provision.get('clusterType'));

    // for each entry in provision
    const propertyList = []; //extra list entries not in config to be added to result once accumulated
    const fields = Object.entries(provisionObj).reduce((result, [key, value]) => {
      if (key === 'yarnProps') {
        // for each entry in yarnProps
        result = Object.entries(value).reduce((subResult, [subKey, subValue]) => {
          if (subKey === 'subPropertyList') {
            YarnForm.mapSubPropertyListToFormFields(subResult, subValue, propsAsFields, propertyList);
          } else if (subKey === 'memoryMB') {
            // add simple yarnProps value (memoryMB is shown in GB)
            subResult[subKey] = NumberFormatUtils.roundNumberField(subValue / 1024);
          } else {
            subResult[subKey] = subValue;
          }
          return subResult; //accumulator in reduce
        }, result);
      } else {
        // add simple provision value
        result[key] = value;
      }
      return result; //accumulator in reduce
    }, {});
    return {
      ...fields,
      propertyList
    };
  }

  static mapSubPropertyListToFormFields = (accumulator, subPropertyList, propsAsFields, extraPropList) => {
    // mutates accumulator and possibly adds entries to extraPropList
    subPropertyList.forEach(({key, value, type}) => {
      const propsAsFieldEntry = propsAsFields.find(prop => prop.key === key);
      if (propsAsFieldEntry) {
        const fieldName = propsAsFieldEntry.field;
        if (propsAsFieldEntry.isArray) {
          // use parser to vet the value; check that the value is an array; assign to configured field
          const parsedValue = HOCON.parse(`value: ${value}`).value;
          if (Array.isArray(parsedValue)) {
            accumulator[fieldName] = parsedValue;
          } else {
            console.error(`${fieldName} was not an array`);
            // someone forced an invalid HOCON array into the system! This should be impossible via the FE, but
            // since this value is just a string for most of its life it isn't validated as it normally would.
            // (This could happen to someone using the API directly.)
            // For now, just reset the value to `['']` so that the user has to re-enter
            accumulator[fieldName] = '';
          }
        } else {
          // assign sub property value to the configured field
          accumulator[fieldName] = value;
        }
      } else {
        YarnForm.addToPropertyList(extraPropList, key, value, type);
      }
    });
  };

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
  prepareSubPropertyForSave = (values) => {
    const { provision } = this.props;
    const propsAsFields = YarnForm.getPropsAsFields(provision && provision.get('clusterType'));
    const subProps = propsAsFields.map(prop => {
      const value = values[prop.field];
      return {
        key: prop.key,
        value: prop.isArray ? JSON.stringify(value) : value
      };
    });
    if (values.propertyList) {
      return [...subProps, ...(values.propertyList.map(v => ({key: v.name, value: v.value, type: v.type})))];
    }
    return subProps;
  };
  //TODO DRY with EC2Form
  prepareValuesForSave = values => {
    const payload = {
      clusterType: 'YARN',
      name: values.nodeTag,
      dynamicConfig: {
        containerCount: values.dynamicConfig.containerCount
      },
      awsProps: null,
      yarnProps: {
        memoryMB: values.memoryMB * 1024,
        virtualCoreCount: values.virtualCoreCount,
        distroType: values.distroType,
        isSecure: values.isSecure,
        queue: values.queue,
        subPropertyList: this.prepareSubPropertyForSave(values)
      }
    };
    const {provision} = this.props;
    if (isEditMode(provision)) {
      payload.id = provision.get('id');
      payload.tag = provision.get('tag');
      payload.desiredState = provision.get('desiredState');
    }
    return payload;
  };

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

  submitForm = (values) => {
    const {provision, dirty} = this.props;
    return this.props.onFormSubmit(this.prepareValuesForSave(values), isRestartRequired(provision, dirty));
  };

  render() {
    const { fields, handleSubmit, style, provision, dirty } = this.props;
    const confirmText = isRestartRequired(provision, dirty) ? la('Restart') : la('Save & Launch');
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
                <div style={label}>{la('Hadoop Engine')}</div>
                <Select
                  name='distroType'
                  items={this.getDistributionOptions()}
                  disabled={isEditMode(provision)}
                  {...fields.distroType}
                />
              </div>
            </div>
            <Checkbox
              style={{paddingTop: 26}}
              label={la('This is a secure engine')}
              disabled={isEditMode(provision)}
              {...fields.isSecure}
            />
          </div>
          <div style={styles.formRow}>
            <FieldWithError
              labelStyle={formLabel}
              style={styles.inlineBlock}
              label={la('Engine Name')}
              errorPlacement='top'
              {...fields.nodeTag}>
              <TextField {...fields.nodeTag}/>
            </FieldWithError>
          </div>
          <div style={styles.formRow}>
            <FieldWithError
              style={{...styles.inlineBlock, marginRight: inputSpacingCssValue}}
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
                <TextField {...fields.memoryMB} style={{width: 75, marginRight: 5}}/>
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
          {this.renderCacheSection(sectionTitle, styles, formLabel, fields)}
        </FormBody>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { provision } = props;
  const initialValues = {
    ...props.initialValues,
    clusterType: 'YARN'
  };
  if (provision.size) {
    return {
      initialValues: {
        ...props.initialValues,
        ...YarnForm.getInitValuesFromProvision(provision)
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
    virtualCoreCount: DEFAULT_CORES,
    ...INIT_VALUES
  }
}, [], mapStateToProps, null)(YarnForm);


const styles = {
  formRow: {
    ...formRow,
    display: 'flex'
  },
  inlineBlock: {
    display: 'inline-block',
    paddingRight: 5
  }
};
