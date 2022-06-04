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
import { PureComponent } from 'react';
import moment from 'moment';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import EntityLink from '@app/pages/HomePage/components/EntityLink';
import EllipsedText from 'components/EllipsedText';

import LinkButton from 'components/Buttons/LinkButton';
import ResourcePin from 'components/ResourcePin';
import AllSourcesMenu from 'components/Menus/HomePage/AllSourcesMenu';
import FontIcon from 'components/Icon/FontIcon';

import SettingsBtn from 'components/Buttons/SettingsBtn';
import { SortDirection } from 'components/VirtualizedTableViewer';
import { getIconStatusDatabase } from 'utils/iconUtils';

import * as allSpacesAndAllSources from 'uiTheme/radium/allSpacesAndAllSources';
import BrowseTable from '../../components/BrowseTable';
import { tableStyles } from '../../tableStyles';

@injectIntl
@Radium
export default class AllSourcesView extends PureComponent {
  static propTypes = {
    sources: PropTypes.object,
    intl: PropTypes.object.isRequired,
    title: PropTypes.string.isRequired,
    isExternalSource: PropTypes.bool,
    isDataPlaneSource: PropTypes.bool
  }

  static contextTypes = {
    location: PropTypes.object.isRequired,
    loggedInUser: PropTypes.object.isRequired
  }

  getTableData() {
    const [ name, datasets, created, action ] = this.getTableColumns();
    return this.props.sources.toList().sort((a, b) => b.get('isActivePin') - a.get('isActivePin')).map((item) => {
      const icon = <FontIcon type={getIconStatusDatabase(item.getIn(['state', 'status']))}/>;
      return { // todo: loc
        rowClassName: item.get('name'),
        data: {
          [name.key]: {
            node: () => (
              <div style={allSpacesAndAllSources.listItem}>
                {icon}
                <EntityLink entityId={item.get('id')}>
                  <EllipsedText text={item.get('name')} />
                </EntityLink>
                <ResourcePin entityId={item.get('id')} />
              </div>
            ),
            value(sortDirection = null) { // todo: DRY
              if (!sortDirection) return item.get('name');
              const activePrefix = sortDirection === SortDirection.ASC ? 'a' : 'z';
              const inactivePrefix = sortDirection === SortDirection.ASC ? 'z' : 'a';
              return (item.get('isActivePin') ? activePrefix : inactivePrefix) + item.get('name');
            }
          },
          [datasets.key]: {
            node: () => item.get('numberOfDatasets')
          },
          [created.key]: {
            node: () => moment(item.get('ctime')).format('MM/DD/YYYY'),
            value: new Date(item.get('ctime'))
          },
          [action.key]: {
            node: () => (
              <span className='action-wrap'>
                <SettingsBtn
                  routeParams={this.context.location.query}
                  menu={<AllSourcesMenu item={item}/>}
                  dataQa={item.get('name')}
                />
              </span>
            )
          }
        }
      };
    });
  }

  getTableColumns() {
    const { intl } = this.props;
    return [
      { key: 'name', label: intl.formatMessage({ id: 'Common.Name' }), flexGrow: 1 },
      { key: 'datasets', label: intl.formatMessage({ id: 'Common.Datasets' }) },
      { key: 'created', label: intl.formatMessage({ id: 'Common.Created' }) },
      {
        key: 'action',
        label: intl.formatMessage({ id: 'Common.Action' }),
        style: tableStyles.actionColumn,
        disableSort: true,
        width: 60
      }
    ];
  }

  renderAddButton() {
    const { isExternalSource, isDataPlaneSource } = this.props;

    /*eslint no-nested-ternary: "off"*/
    const headerId = isExternalSource ? 'Source.AddExternalSource' :
      isDataPlaneSource ? 'Source.AddDataPlane' : 'Source.AddDataLake';

    return (
      this.context.loggedInUser.admin && <LinkButton
        buttonStyle='primary'
        to={{ ...this.context.location, state: { modal: 'AddSourceModal', isExternalSource, isDataPlaneSource }}}
        style={allSpacesAndAllSources.addButton}>
        {this.props.intl.formatMessage({ id: headerId })}
      </LinkButton>
    );
  }


  render() {
    const { sources, title } = this.props;
    const totalItems = sources.size;
    return (
      <BrowseTable
        title={`${title} (${totalItems})`}
        tableData={this.getTableData()}
        columns={this.getTableColumns()}
        buttons={this.renderAddButton()}
      />
    );
  }
}
