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
import {Component} from 'react';
import PropTypes from 'prop-types';
import uuid from 'uuid';

import FieldList, {AddButton, RemoveButton} from 'components/Fields/FieldList';
import YarnProperty from 'components/Forms/YarnProperty';
import {description, section, sectionTitle} from 'uiTheme/radium/forms';

PropertyItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func
};

// todo: chris is wondering where `style` is actually populated
// todo: chris also curious why the `PropertyItem` wrapper is needed (couldn't `Property` just own all of this)
function PropertyItem({style, item, onRemove}) {
  return (
    <div className='property-item' style={{...styles.item, style, paddingBottom: 10}}>
      <YarnProperty fields={item} />
      {onRemove && <RemoveButton onClick={onRemove} style={styles.removeButton}/> }
    </div>
  );
}

export default class YarnProperties extends Component {
  static getFields() {
    return YarnProperty.getFields().map(field => `propertyList[].${field}`);
  }

  static propTypes = {
    fields: PropTypes.object,
    title: PropTypes.string,
    emptyLabel: PropTypes.string,
    addLabel: PropTypes.string,
    description: PropTypes.string
  };

  static defaultProps = { // todo: `la` failing to build here
    title: ('Properties'),
    emptyLabel: ('(No Properties Added)'),
    addLabel: ('Add Property')
  };

  static validate(values) {
    return {
      propertyList: values.propertyList.map((property) => {
        return YarnProperty.validate(property);
      })
    };
  }
  //
  // Handlers
  //

  addItem = (e) => {
    const {fields: {propertyList}} = this.props;
    e.preventDefault();
    propertyList.addField({id: uuid.v4(), name: '', value: '', type: 'JAVA_PROP'});
  }

  render() {
    const {fields: {propertyList}, title, emptyLabel, addLabel} = this.props;
    const des = this.props.description ? <div style={description}>{this.props.description}</div> : null;
    return (
      <div className='properties' style={section}>
        <h2 style={sectionTitle}>{title}</h2>
        {des}
        <FieldList
          items={propertyList}
          itemHeight={50}
          getKey={item => item.id.value}
          emptyLabel={emptyLabel}>
          <PropertyItem/>
        </FieldList>

        <AddButton addItem={this.addItem} style={{marginLeft: -5}}>{addLabel}</AddButton>
      </div>
    );
  }
}

const styles = {
  item: {
    display: 'flex',
    alignItems: 'center',
    paddingRight: 14,
    marginRight: -14
  },
  removeButton: {
    marginLeft: 10,
    marginTop: 16,
    marginRight: -35
  }
};
