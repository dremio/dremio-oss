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
import { Component } from "react";
import PropTypes from "prop-types";
import { v4 as uuidv4 } from "uuid";
import { get, set } from "lodash/object";

import FieldList, {
  AddButton,
  RemoveButton,
} from "components/Fields/FieldList";
import {
  sectionTitle,
  description as descriptionStyle,
} from "uiTheme/radium/forms";
import FormUtils from "#oss/utils/FormUtils/FormUtils";

import Host from "./Host";

const { CONFIG_PROP_NAME, addFormPrefixToPropName } = FormUtils;

const defaultPropName = addFormPrefixToPropName("hostList");

HostItem.propTypes = {
  style: PropTypes.object,
  item: PropTypes.object,
  onRemove: PropTypes.func,
};

function HostItem({ style, item, onRemove }) {
  return (
    <div className="host-item" style={{ ...styles.item, ...style }}>
      <Host fields={item} />
      {onRemove && (
        <RemoveButton onClick={onRemove} style={styles.removeButton} />
      )}
    </div>
  );
}

function getKey(item) {
  return item.id.value;
}

function validateHostList(values, elementConfig) {
  const propertyName = elementConfig
    ? elementConfig.propertyName
    : defaultPropName;
  const hostList = get(values, propertyName);
  const result = {};
  if (
    !hostList ||
    hostList.length === 0 ||
    (hostList.length === 1 && !hostList[0].hostname)
  ) {
    set(result, propertyName, [{ hostname: "At least one host is required." }]);
    return result;
  }

  set(
    result,
    propertyName,
    hostList.map((host) => Host.validate(host)),
  );
  return result;
}

export default class HostList extends Component {
  static getFields(elementConfig) {
    const propName = elementConfig ? elementConfig.propName : defaultPropName;
    return Host.getFields().map((key) => `${propName}[].${key}`);
  }

  static getNewHost(port) {
    return { id: uuidv4(), port };
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    defaultPort: PropTypes.number,
    description: PropTypes.node,
    single: PropTypes.bool,
    title: PropTypes.string,
    elementConfig: PropTypes.object,
  };

  static getValidators(elementConfig) {
    return function (values) {
      return validateHostList(values, elementConfig);
    };
  }

  static validate(values) {
    const {
      [CONFIG_PROP_NAME]: { hostList },
    } = values;
    if (!hostList || hostList.length === 0) {
      return {
        [CONFIG_PROP_NAME]: { hostList: "At least one host is required." },
      };
    }

    return {
      [CONFIG_PROP_NAME]: {
        hostList: hostList.map((host) => {
          return Host.validate(host);
        }),
      },
    };
  }

  constructor(props) {
    super(props);
    this.addItem = this.addItem.bind(this);
  }

  addItem(e) {
    e.preventDefault();
    const defaultPort =
      this.props.defaultPort || this.props.elementConfig.default_port || 9200;
    get(this.props.fields, this.props.elementConfig.propertyName).addField(
      HostList.getNewHost(defaultPort),
    );
  }

  render() {
    const { fields, single, title } = this.props;
    const { elementConfig } = this.props;
    const fieldItems = get(fields, elementConfig.propertyName);
    const description = this.props.description ? (
      <div style={styles.des}>{this.props.description}</div>
    ) : null;
    const addHost = !single ? (
      <AddButton style={styles.addButton} addItem={this.addItem}>
        {laDeprecated("Add host")}
      </AddButton>
    ) : null;
    return (
      <div className="hosts">
        {!elementConfig && (
          <h2 style={sectionTitle}>{title || laDeprecated("Hosts")}</h2>
        )}
        {description}
        <FieldList
          items={fieldItems}
          itemHeight={50}
          getKey={getKey}
          minItems={1}
        >
          <HostItem />
        </FieldList>
        {addHost}
      </div>
    );
  }
}

const styles = {
  item: {
    display: "flex",
    alignItems: "center",
    paddingRight: 14,
    marginRight: -14,
    marginBottom: -10,
  },
  des: {
    ...descriptionStyle,
    marginBottom: 15,
  },
  addButton: {
    marginLeft: -3,
    marginTop: -13,
    marginBottom: 10,
  },
  removeButton: {
    marginTop: "24px",
    paddingLeft: "16px",
  },
};
