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
import Radium from 'radium';
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';

import { formLabel, formDefault } from 'uiTheme/radium/typography';
import { PALE_BLUE, RED } from 'uiTheme/radium/colors';
import SelectFrequentValues from 'components/Fields/SelectFrequentValues';
import { SearchField } from 'components/Fields';

import { applyValidators, notEmptyObject } from 'utils/validation';

import CardFooter from './../CardFooter';

export const MIN_VALUES_TO_SHOW_SEARCH = 6;

export const MAX_SUGGESTIONS = 100;

@Radium
@pureRender
export default class ReplaceValues extends Component {

  static getFields() {
    return ['replaceValues'];
  }

  static validate(values) {
    const errors = applyValidators(values, [
      notEmptyObject('replaceValues', 'Select at least one value to replace.')
    ]);
    return errors;
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    sqlSize: PropTypes.number,
    valueOptions: PropTypes.instanceOf(Immutable.Map)
  };

  static defaultProps = {
    valueOptions: Immutable.fromJS({ values: [] })
  };

  constructor(props) {
    super(props);
    this.handleAllClick = this.handleAllClick.bind(this);
    this.handleNoneClick = this.handleNoneClick.bind(this);
  }

  state = {
    filter: ''
  }

  handleFilter = (filter) => {
    this.setState({
      filter
    });
  }

  filterValuesList = (values) => {
    const { filter } = this.state;
    if (!filter) {
      return values;
    }
    return values.filter((item) =>
      // gaurd on value (we get no value for `null` cases): DX-6985
      item.has('value') && item.get('value').toLowerCase().includes(filter.trim().toLowerCase())
    );
  }

  handleAllClick(e) {
    e.preventDefault();
    const { valueOptions, fields } = this.props;
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values'));
    const valuesList = filteredValues.map((value) => value.get('value')).slice(0, MAX_SUGGESTIONS);
    const form = valuesList.reduce((prev, curr) => {
      return { ...prev, [curr]: true };
    }, {});
    fields.replaceValues.onChange(form);
  }

  handleNoneClick(e) {
    e.preventDefault();
    const { fields } = this.props;
    fields.replaceValues.onChange({});
  }

  renderSearchField() {
    const { valueOptions } = this.props;
    return valueOptions && valueOptions.get('values').size > MIN_VALUES_TO_SHOW_SEARCH
      ? <SearchField
        showCloseIcon
        placeholder={la('Search values...')}
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
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values'));
    if (!fields.replaceValues || !fields.replaceValues.value) {
      return;
    }
    const numSelected = Object.values(fields.replaceValues.value).filter(Boolean).length;
    const text = numSelected === 0
      ? la('Please select at least one value.')
      : `${numSelected} of ${filteredValues.size} selected.`;
    return (
      <em style={numSelected === 0
        ? styles.noSelectValue
        : {...styles.noSelectValue, color: '#333'}}> {text} </em>
    );
  }

  render() {
    const { valueOptions } = this.props;
    const filteredValues = this.filterValuesList(valueOptions && valueOptions.get('values'));
    const footerStyle = { width: styles.valuesWrap.width, backgroundColor: PALE_BLUE };
    return (
      <div style={styles.base}>
        <div style={styles.header}>
          Available Values ({filteredValues.size})
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
  noSelectValue: {
    flex: 1,
    textAlign: 'right',
    color: RED
  }
};
