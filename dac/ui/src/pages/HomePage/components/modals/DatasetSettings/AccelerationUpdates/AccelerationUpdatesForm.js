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
import { Component, PropTypes } from 'react';
import Immutable from 'immutable';
import Radium from 'radium';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, modalFormProps, FormBody } from 'components/Forms';
import { body } from 'uiTheme/radium/typography';
import { section, label } from 'uiTheme/radium/forms';
import { Radio, FieldSelect } from 'components/Fields';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import HoverHelp from 'components/HoverHelp';
import { FormTitle } from 'components/Forms';

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
    entityType: PropTypes.string
  };

  whyCannotUseIncremental() {
    if (this.props.entityType === 'physicalDataset' && !this.props.datasetFields.size) {
      return la('Incremental updating is not available for datasets without any number or date fields.');
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
      accelerationGracePeriod: values.accelerationGracePeriod
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
      <div style={body}>
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
          <FormTitle>{la('Refresh Policy')}</FormTitle>
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
    alignItems: 'center'
  }
};

const mapStateToProps = (state, ownProps) => {
  const settings = ownProps.accelerationSettings || Immutable.Map({});
  const accelerationRefreshPeriod = settings.get('accelerationRefreshPeriod') || DataFreshnessSection.defaultFormValueRefreshInterval();
  const accelerationGracePeriod = settings.get('accelerationGracePeriod') || DataFreshnessSection.defaultFormValueGracePeriod();
  return {
    initialValues: {
      method: settings.get('method') || 'FULL',
      refreshField: settings.get('refreshField') || ownProps.datasetFields.getIn([0, 'name']) || '',
      accelerationRefreshPeriod,
      accelerationGracePeriod
    }
  };
};

export default connectComplexForm({
  form: 'accelerationUpdatesForm',
  fields: FIELDS
}, SECTIONS, mapStateToProps, null)(AccelerationUpdatesForm);
