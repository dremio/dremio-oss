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
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { FormBody, FormTitle, ModalForm, modalFormProps } from 'components/Forms';
import { label, section } from 'uiTheme/radium/forms';
import { FieldSelect, Radio } from 'components/Fields';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import HoverHelp from 'components/HoverHelp';

const SECTIONS = [DataFreshnessSection];

const INCREMENTAL = 'INCREMENTAL';
const FULL = 'FULL';
const FIELDS = ['method', 'refreshField'].concat(DataFreshnessSection.getFields());

@Radium
export class AccelerationUpdatesForm extends Component {
  static propTypes = {
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    handleSubmit: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    location: PropTypes.object,
    fields: PropTypes.object,
    values: PropTypes.object,
    errors: PropTypes.object,
    accelerationSettings: PropTypes.instanceOf(Immutable.Map),
    datasetFields: PropTypes.instanceOf(Immutable.List),
    entityType: PropTypes.string,
    entityId: PropTypes.string
  };

  whyCannotUseIncremental() {
    if (this.props.entityType === 'physicalDataset' && !this.props.datasetFields.size) {
      return la('Incremental updating is not available for datasets without any Int, BigInt, Decimal, Float, Double, Varchar, Date, or Timestamp fields.');
    }
    if (this.props.entityType === 'file') {
      return la('Incremental updating is not available for file-based datasets.');
    }
    return undefined;
  }

  canUseIncremental() {
    return !this.whyCannotUseIncremental();
  }

  requiresIncrementalFieldSelection(values) {
    return values.method === INCREMENTAL && this.props.entityType === 'physicalDataset';
  }

  mapFormValues(values) {
    const { refreshField } = values;
    const requiredValues = {
      method: values.method,
      accelerationRefreshPeriod: values.accelerationRefreshPeriod,
      accelerationGracePeriod: values.accelerationGracePeriod,
      accelerationNeverExpire: values.accelerationNeverExpire,
      accelerationNeverRefresh: values.accelerationNeverRefresh
    };

    if (this.requiresIncrementalFieldSelection(values)) {
      return {
        ...requiredValues,
        fieldList: [refreshField],
        refreshField
      };
    }
    return requiredValues;
  }

  submitForm = (values) => {
    return this.props.submit(this.mapFormValues(values));
  }

  renderContent() {
    const { fields, values } = this.props;
    const helpContent = la('Refresh method for Reflections using data from this dataset.');
    const incrementalLabel = this.props.entityType === 'folder'
      ? la('Incremental update based on new files')
      : la('Incremental update');
    return (
      <div>
        <div style={section}>
          <span style={styles.label}>
            {la('Refresh Method')}
            <HoverHelp content={helpContent} />
          </span>
          <div style={styles.items}>
            <Radio
              {...fields.method}
              radioValue={FULL}
              style={styles.margin}
              label={la('Full update')}
            />
            <Radio
              {...fields.method}
              radioValue={INCREMENTAL}
              style={styles.margin}
              disabled={!this.canUseIncremental()}
              label={incrementalLabel}
            />
            <i>{this.whyCannotUseIncremental()}</i>
            {this.requiresIncrementalFieldSelection(values) ?
              <label style={styles.fieldSelectWrap}>
                <span style={label}>{la('Identify new records using the field:')}</span>
                <FieldSelect
                  formField={fields.refreshField}
                  style={styles.fieldSelect}
                  items={this.props.datasetFields.toJS()} />
              </label> : null}
          </div>
        </div>
        <DataFreshnessSection
          fields={fields}
          entityType='dataset'
          datasetId={this.props.entityId}
        />
      </div>
    );
  }

  render() {
    const { handleSubmit, onCancel } = this.props;
    return (
      <ModalForm
        {...modalFormProps(this.props)}
        onSubmit={handleSubmit(this.submitForm)}
        onCancel={onCancel}
      >
        <FormBody>
          <FormTitle>{la('Reflection Refresh')}</FormTitle>
          {this.renderContent()}
        </FormBody>
      </ModalForm>
    );
  }
}

const styles = {
  margin: {
    marginBottom: 5
  },
  fieldSelectWrap: {
    display: 'flex',
    marginLeft: 23,
    alignItems: 'center'
  },
  fieldSelect: {
    marginLeft: 5
  },
  items: {
    display: 'flex',
    flexDirection: 'column'
  },
  label: {
    ...label,
    display: 'flex',
    alignItems: 'center',
    fontSize: 18,
    fontWeight: 300,
    marginBottom: 10
  }
};

const mapStateToProps = (state, ownProps) => {
  const settings = ownProps.accelerationSettings || Immutable.Map({});

  const accelerationRefreshPeriod = settings.has('accelerationRefreshPeriod') ? settings.get('accelerationRefreshPeriod') : DataFreshnessSection.defaultFormValueRefreshInterval();
  const accelerationGracePeriod = settings.has('accelerationGracePeriod') ? settings.get('accelerationGracePeriod') : DataFreshnessSection.defaultFormValueGracePeriod();
  const accelerationNeverExpire = settings.has('accelerationNeverExpire') ? settings.get('accelerationNeverExpire') : false;
  const accelerationNeverRefresh = settings.has('accelerationNeverRefresh') ? settings.get('accelerationNeverRefresh') : false;

  return {
    initialValues: {
      method: settings.get('method') || 'FULL',
      refreshField: settings.get('refreshField') || ownProps.datasetFields.getIn([0, 'name']) || '',
      accelerationRefreshPeriod,
      accelerationGracePeriod,
      accelerationNeverExpire,
      accelerationNeverRefresh
    }
  };
};

export default connectComplexForm({
  form: 'accelerationUpdatesForm',
  fields: FIELDS
}, SECTIONS, mapStateToProps, null)(AccelerationUpdatesForm);

