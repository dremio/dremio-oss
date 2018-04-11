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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { formLabel, formDefault } from 'uiTheme/radium/typography';
import { PALE_BLUE } from 'uiTheme/radium/colors';
import SelectFrequentValues from 'components/Fields/SelectFrequentValues';
import { SearchField } from 'components/Fields';

import CardFooter from './../CardFooter';

export const MIN_VALUES_TO_SHOW_SEARCH = 6;

export const MAX_SUGGESTIONS = 100;

// todo: loc

@Radium
@pureRender
export default class ReplaceValues extends Component {

  static getFields() {
    return ['replaceValues'];
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    sqlSize: PropTypes.number,
    valueOptions: PropTypes.instanceOf(Immutable.Map)
  };

  static defaultProps = {
    valueOptions: Immutable.fromJS({ values: [] })
  };

  state = {
    filter: ''
  }

  handleFilter = (filter) => {
    this.setState({
      filter
    });
  }

  filterValuesList = (values) => {
    values = values.slice(0, MAX_SUGGESTIONS);
    const { filter } = this.state;
    if (!filter) {
      return values;
    }
    return values.filter((item) =>
      // gaurd on value (we get no value for `null` cases): DX-6985
      item.has('value') && item.get('value').toLowerCase().includes(filter.trim().toLowerCase())
    );
  }

  handleAllClick = (e) => {
    e.preventDefault();
    const { valueOptions, fields } = this.props;
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values')).map((value) => value.get('value')).toJS();
    const valuesSet = new Set(fields.replaceValues.value);
    filteredValues.forEach(valuesSet.add, valuesSet);
    fields.replaceValues.onChange([...valuesSet]);
  }

  handleNoneClick = (e) => {
    e.preventDefault();
    const { valueOptions, fields } = this.props;
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values')).map((value) => value.get('value')).toJS();
    const valuesSet = new Set(fields.replaceValues.value);
    filteredValues.forEach(valuesSet.delete, valuesSet);
    fields.replaceValues.onChange([...valuesSet]);
  }

  renderSearchField() {
    const { valueOptions } = this.props;
    return valueOptions && valueOptions.get('values').size > MIN_VALUES_TO_SHOW_SEARCH
      ? <SearchField
        showCloseIcon
        placeholder={la('Search values…')}
        value={this.state.filter}
        onChange={this.handleFilter}
        />
      : null;
  }

  renderValuesList(values) {
    const { fields } = this.props;
    return values.size
      ? <SelectFrequentValues
        options={values.toJS()}
        field={fields.replaceValues}
        style={styles.valuesList}
        />
      : <div style={styles.notFound}>{la('Not found')}</div>;
  }

  renderSelectedValuesCount() {
    const { fields, valueOptions } = this.props;
    if (!fields.replaceValues || !fields.replaceValues.value) {
      return;
    }
    const numSelected = fields.replaceValues.value.length;
    const text = `${numSelected} of ${valueOptions.get('values').size} selected`; // todo: loc
    return <em style={styles.numSelected}>{text}</em>;
  }

  render() {
    const { valueOptions } = this.props;
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values'));
    const footerStyle = { width: styles.valuesWrap.width, backgroundColor: PALE_BLUE };
    return (
      <div style={styles.base}>
        <div style={styles.header}>
          {la('Available Values')}
          <span style={styles.bulkActions}>Select:
            <a style={styles.bulkAction} onClick={this.handleAllClick}>
              {la('All')}
            </a> | <a style={styles.bulkAction} onClick={this.handleNoneClick}>{la('None')}</a>
          </span>
          {this.renderSelectedValuesCount()}
        </div>
        <div style={{ ...styles.valuesWrap }}>
          {this.renderSearchField()}
          {this.renderValuesList(filteredValues)}
        </div>
        <CardFooter card={valueOptions} style={[footerStyle, { padding: '5px 0' }]}/>
      </div>
    );
  }
}

const styles = {
  base: {
    minWidth: 800,
    margin: '0 10px 10px',
    position: 'relative'
  },
  header: {
    ...formLabel,
    lineHeight: '24px',
    display: 'flex',
    width: 600
  },
  bulkActions: {
    ...formDefault,
    marginLeft: 20
  },
  bulkAction: {
    margin: '0 3px'
  },
  valuesWrap: {
    display: 'flex',
    flexDirection: 'column',
    height: 160,
    background: '#fff',
    border: '1px solid rgba(0,0,0,0.1)',

    padding: '5px 10px',
    width: 600
  },
  valuesList: {
    flex: 1,
    overflow: 'auto',
    paddingRight: 10
  },
  notFound: {
    padding: '10px 10px'
  },
  numSelected: {
    flex: 1,
    textAlign: 'right',
    color: '#333'
  }
};
