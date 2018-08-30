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
import uuid from 'uuid';

import FieldList, {AddButton, RemoveButton, RemoveButtonStyles } from 'components/Fields/FieldList';
import { sectionTitle, description as descriptionStyle } from 'uiTheme/radium/forms';

import Host from './Host';

HostItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func
};

function HostItem({style, item, onRemove}) {
  return (
    <div className='host-item' style={{...styles.item, ...style}}>
      <Host fields={item}/>
      {onRemove && <RemoveButton onClick={onRemove} style={styles.removeButton}/> }
    </div>
  );
}

function getKey(item) {
  return item.id.value;
}

function validateHostList(values, elementConfig) {
  const propertyName = (elementConfig) ? elementConfig.propertyName : 'hostList';
  const {config} = values;
  const hostList = config[propertyName];
  const result = {config: {}};
  if (!hostList || hostList.length === 0 || (hostList.length === 1 && !hostList[0].hostname)) {
    result.config[propertyName] = [{hostname: 'At least one host is required.'}];
    return result;
  }

  result.config[propertyName] = hostList.map(host => Host.validate(host));
  return result;
}

export default class HostList extends Component {
  static getFields(elementConfig) {
    const propName = (elementConfig) ? elementConfig.propName : 'config.hostList';
    return Host.getFields().map(key => `${propName}[].${key}`);
  }

  static getNewHost(port) {
    return {id: uuid.v4(), port};
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    defaultPort: PropTypes.number,
    description: PropTypes.node,
    single: PropTypes.bool,
    title: PropTypes.string,
    elementConfig: PropTypes.object
  };

  static getValidators(elementConfig) {
    return function(values) {
      return validateHostList(values, elementConfig);
    };
  }

  static validate(values) {
    const {config: {hostList}} = values;
    if (!hostList || hostList.length === 0) {
      return {config: {hostList: 'At least one host is required.'}};
    }

    return {config: {hostList: hostList.map((host) => {
      return Host.validate(host);
    })}};
  }

  constructor(props) {
    super(props);
    this.addItem = this.addItem.bind(this);
  }

  addItem(e) {
    e.preventDefault();
    const defaultPort = this.props.defaultPort || this.props.elementConfig.default_port || 9200;
    this.props.fields.config[this.props.elementConfig.propertyName].addField(HostList.getNewHost(defaultPort));
  }

  render() {
    const {fields, single, title} = this.props;
    const {elementConfig} = this.props;
    const fieldItems = fields.config[elementConfig.propertyName];
    const description = this.props.description ? <div style={styles.des}>{this.props.description}</div> : null;
    const addHost = !single
      ? <AddButton style={styles.addButton} addItem={this.addItem}>{la('Add host')}</AddButton>
      : null;
    return (
      <div className='hosts'>
        {!elementConfig && <h2 style={sectionTitle}>{title || la('Hosts')}</h2>}
        {description}
        <FieldList
          items={fieldItems}
          itemHeight={50}
          getKey={getKey}
          minItems={1}>
          <HostItem/>
        </FieldList>
        {addHost}
      </div>
    );
  }
}

const styles = {
  item: {
    display: 'flex',
    alignItems: 'center',
    paddingRight: 14,
    marginRight: -14,
    marginBottom: -10
  },
  des: {
    ...descriptionStyle,
    marginBottom: 15
  },
  addButton: {
    marginLeft: -3,
    marginTop: -13,
    marginBottom: 10
  },
  removeButton: {
    ...RemoveButtonStyles.inline,
    marginTop: '14px',
    marginBottom: '12px'
  }
};
