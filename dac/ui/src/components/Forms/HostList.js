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
import uuid from 'uuid';

import FieldList, {AddButton, RemoveButton} from 'components/Fields/FieldList';
import {body} from 'uiTheme/radium/typography';
import { section, sectionTitle, description as descriptionStyle } from 'uiTheme/radium/forms';

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

export default class HostList extends Component {
  static getFields() {
    return Host.getFields().map(key => `config.hostList[].${key}`);
  }

  static getNewHost(port) {
    return {id: uuid.v4(), port};
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    defaultPort: PropTypes.number,
    description: PropTypes.node,
    single: PropTypes.bool,
    title: PropTypes.string
  };

  static validate(values) {
    const {config: {hostList}} = values;
    if (!hostList || hostList.length === 0) {
      return {config: {hostList: 'At least one host is required'}};
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
    this.props.fields.config.hostList.addField(HostList.getNewHost(this.props.defaultPort));
  }

  render() {
    const {fields, single, title} = this.props;
    const description = this.props.description ? <div style={styles.des}>{this.props.description}</div> : null;
    const addHost = !single
      ? <AddButton style={styles.addButton} addItem={this.addItem}>{la('Add Host')}</AddButton>
      : null;
    return (
      <div className='hosts' style={section}>
        <h3 style={sectionTitle}>{title || la('Hosts')}</h3>
        {description}
        <FieldList items={fields.config.hostList} itemHeight={50} getKey={getKey} minItems={1}>
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
    ...body,
    ...descriptionStyle,
    marginBottom: 15
  },
  addButton: {
    marginLeft: -5,
    marginTop: -30
  },
  removeButton: {
    marginLeft: 10,
    marginBottom: 12,
    marginRight: -35
  }
};
