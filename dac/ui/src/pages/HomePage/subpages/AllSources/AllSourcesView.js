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
import { Component } from 'react';
import moment from 'moment';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { Link } from 'react-router';
import { injectIntl } from 'react-intl';

import EllipsedText from 'components/EllipsedText';

import LinkButton from 'components/Buttons/LinkButton';
import ResourcePin from 'components/ResourcePin';
import AllSourcesMenu from 'components/Menus/HomePage/AllSourcesMenu';
import FontIcon from 'components/Icon/FontIcon';

import SettingsBtn from 'components/Buttons/SettingsBtn';
import {SortDirection} from 'components/VirtualizedTableViewer';
import { getIconStatusDatabase } from 'utils/iconUtils';

import * as allSpacesAndAllSources from 'uiTheme/radium/allSpacesAndAllSources';
import BrowseTable from '../../components/BrowseTable';
import { tableStyles } from '../../tableStyles';

@injectIntl
@Radium
@pureRender
export default class AllSourcesView extends Component {
  static propTypes = {
    sources: PropTypes.object,
    toggleActivePin: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
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
                <Link style={allSpacesAndAllSources.link} to={item.getIn(['links', 'self'])}>
                  <EllipsedText text={item.get('name')} />
                </Link>
                <ResourcePin
                  name={item.get('name')}
                  isActivePin={item.get('isActivePin') || false}
                  toggleActivePin={this.props.toggleActivePin}
                />
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
            node: () => item.get('numberOfDatasets'),
            value: item.get('numberOfDatasets')
          },
          [created.key]: {
            node: () => moment(item.get('created')).format('MM/DD/YYYY'),
            value: new Date(item.get('created'))
          },
          [action.key]: {
            node: () => (
              <span className='action-wrap'>
                <SettingsBtn
                  routeParams={this.context.location.query}
                  menu={<AllSourcesMenu source={item} type={item.get('type')}/>}
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
      { key: 'name', title: intl.formatMessage({ id: 'Common.Name' }), flexGrow: 1 },
      { key: 'datasets', title: intl.formatMessage({ id: 'Dataset.Datasets' }), style: tableStyles.datasetsColumn },
      { key: 'created', title: intl.formatMessage({ id: 'Common.Created' }) },
      {
        key: 'action',
        title: intl.formatMessage({ id: 'Common.Action' }),
        style: tableStyles.actionColumn,
        disableSort: true,
        width: 60
      }
    ];
  }

  renderAddButton() {
    return (
      this.context.loggedInUser.admin && <LinkButton
        buttonStyle='primary'
        to={{ ...location, state: { modal: 'AddSourceModal' }}}
        style={allSpacesAndAllSources.addButton}>
        {this.props.intl.formatMessage({ id: 'Source.AddSource' })}
      </LinkButton>
    );
  }


  render() {
    const { intl, sources } = this.props;
    const totalItems = sources.size;
    return (
      <BrowseTable
        title={`${intl.formatMessage({ id: 'Source.AllSources' })} (${totalItems})`}
        tableData={this.getTableData()}
        columns={this.getTableColumns()}
        buttons={this.renderAddButton()}
      />
    );
  }
}
