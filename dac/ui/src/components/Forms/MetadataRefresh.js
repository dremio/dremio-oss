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
import { label, subSectionTitle, formRow } from 'uiTheme/radium/forms';
import { Select } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';
import DurationInput from './DurationInput';

//todo wrap in la
const OBJECT_NAMES_TOOLTIP = 'Refresh interval for top-level source object names' +
  ' such as names of DBs and tables. This is a lightweight operation.';
const FULL_SCHEMA_TOOLTIP = 'Expiration of metadata Dremio needs for query planning' +
  ' such as information on fields, types, shards, statistics and locality.';
const REFRESH_MODE_TOOLTIP = 'Background - Queried datasets: Dremio updates deep metadata' +
  ' for previously queried stale objects in a source. This mode increases query performance' +
  ' as less work needs to be done at query time for these datasets.\n Background - All datasets:' +
  ' Dremio updates deep metadata for all stale datasets in a source. This mode increases query' +
  ' performance as less work needs to be done at query time.\n As Needed: Dremio updates deep metadata' +
  ' for a dataset at query time. This mode minimizes metadata queries on a source when not used,' +
  ' but might lead to longer planning times.';
const AUTHORIZATION_TOOLTIP = 'Expiration of authorization information that Dremio caches when' +
  ' querying systems with impersonation enabled.';
const DEFAULT_DURATION = 1800000;
const TTL_FIELDS = ['namesRefreshMillis', 'datasetDefinitionTTLMillis', 'authTTLMillis'];

export default class MetadataRefresh extends Component {

  static propTypes = {
    fields: PropTypes.object,
    hideObjectNames: PropTypes.bool
  };

  static defaultProps = {
    hideObjectNames: false
  };

  static defaultFormValues() {
    return {
      metadataPolicy: {
        namesRefreshMillis: DurationInput.constructFields(DEFAULT_DURATION),
        datasetDefinitionTTLMillis: DurationInput.constructFields(DEFAULT_DURATION),
        authTTLMillis: DurationInput.constructFields(DEFAULT_DURATION),
        updateMode: 'PREFETCH_QUERIED'
      }
    };
  }

  static getFields() {
    return [
      'metadataPolicy.namesRefreshMillis.duration',
      'metadataPolicy.namesRefreshMillis.unit',
      'metadataPolicy.datasetDefinitionTTLMillis.duration',
      'metadataPolicy.datasetDefinitionTTLMillis.unit',
      'metadataPolicy.authTTLMillis.duration',
      'metadataPolicy.authTTLMillis.unit',
      'metadataPolicy.updateMode'
    ];
  }

  static mapToFormFields(source) {
    const defaultValues = MetadataRefresh.defaultFormValues();
    const metadataPolicy = source && source.toJS().metadataPolicy || {};
    const initialValue = {
      ...defaultValues.metadataPolicy,
      updateMode: metadataPolicy.updateMode || defaultValues.metadataPolicy.updateMode
    };
    return TTL_FIELDS.reduce((fieldValues, ttlField) => {
      const ttlSourceValue = metadataPolicy[ttlField];
      if (ttlSourceValue) {
        fieldValues[ttlField] = DurationInput.constructFields(ttlSourceValue);
      }
      return fieldValues;
    }, initialValue);
  }

  static normalizeValues(values) {
    return TTL_FIELDS.reduce((metadataValue, ttlField) => {
      return {
        ...metadataValue,
        [ttlField]: DurationInput.convertToMilliseconds(values.metadataPolicy[ttlField])
      };
    }, {updateMode: values.metadataPolicy.updateMode});
  }

  refreshModeOptions = [
    { label: la('Background - Queried Datasets'), option: 'PREFETCH_QUERIED' },
    { label: la('Background - All datasets'), option: 'PREFETCH' },
    { label: la('As Needed'), option: 'INLINE' }
  ];

  render() {
    const { fields: { metadataPolicy }, hideObjectNames } = this.props;
    return (
      <div className='metadata-refresh'>
        <h3 style={subSectionTitle}>{la('Metadata Refresh Policy')}</h3>
        {!hideObjectNames && <div style={formRow}>
          <span style={styles.label}>
            {la('Object Names')}
            <HoverHelp content={la(OBJECT_NAMES_TOOLTIP)} tooltipInnerStyle={styles.hoverTip} />
          </span>
          <DurationInput fields={metadataPolicy.namesRefreshMillis}/>
        </div>}
        <div style={formRow}>
          <span style={{...styles.label, width: 240}}>
            {la('Full Schema')}
            <HoverHelp content={la(FULL_SCHEMA_TOOLTIP)} tooltipInnerStyle={styles.hoverTip} />
          </span>
          <span style={styles.label}>
            {la('Refresh Mode')}
            <HoverHelp content={la(REFRESH_MODE_TOOLTIP)} tooltipInnerStyle={styles.hoverTip} />
          </span>
          <div style={{ display: 'flex' }}>
            <DurationInput fields={metadataPolicy.datasetDefinitionTTLMillis}/>
            <Select
              {...metadataPolicy.updateMode}
              items={this.refreshModeOptions}
              buttonStyle={{ textAlign: 'left' }}
              style={styles.inRowSelect}
            />
          </div>
        </div>
        <div style={formRow}>
          <span style={styles.label}>
            {la('Authorization')}
            <HoverHelp content={la(AUTHORIZATION_TOOLTIP)} tooltipInnerStyle={styles.hoverTip}/>
          </span>
          <DurationInput fields={metadataPolicy.authTTLMillis}/>
        </div>
      </div>
    );
  }
}

const styles = {
  select: {
    width: 164
  },
  numberInput: {
    width: 42
  },
  inRowSelect: {
    width: 200,
    marginLeft: 20
  },
  hoverTip: {
    textAlign: 'left',
    width: 300,
    whiteSpace: 'pre-line'
  },
  label: {
    ...label,
    display: 'inline-flex',
    alignItems: 'center'
  }
};
