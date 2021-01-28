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
export const getTableData = () => {
  if (!this.props.provision) return new Immutable.List();

  const getRows = (key, status) => {
    const items = this.props.provision.getIn(['containers', key]);
    if (!items) return new Immutable.List();

    return items.map((item) => {
      const containerPropertyList = item.getIn(['containerPropertyList']);
      const row = {
        rowClassName: '',
        data: containerPropertyList.reduce((prev, property) => {
          return {...prev, [property.get('key')]: property.get('value')};
        }, {status})
      };
      return row;
    });
  };

  // Note: 'Running' is not a term we use elsewhere in the UI
  // but in this list we can't distinguish "Active" from "Decomissioning"
  const runningData = getRows('runningList', la('Running'));
  const disconnectedData = getRows('disconnectedList', la('Provisioning or Disconnected'));

  return disconnectedData.concat(runningData);
};
