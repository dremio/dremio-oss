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
import { PureComponent, Fragment } from 'react';
import { connect } from 'react-redux';
import moment from 'moment';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import EntityLink from '@app/pages/HomePage/components/EntityLink';
import SpacesLoader from '@app/pages/HomePage/components/SpacesLoader';
import { RestrictedArea } from '@app/components/Auth/RestrictedArea';
import { manageSpaceRule } from '@app/utils/authUtils';

import * as allSpacesAndAllSources from 'uiTheme/radium/allSpacesAndAllSources';

import EllipsedText from 'components/EllipsedText';
import { getSpaces } from 'selectors/resources';

import LinkButton from 'components/Buttons/LinkButton';
import ResourcePin from 'components/ResourcePin';
import AllSpacesMenu from 'components/Menus/HomePage/AllSpacesMenu';
import FontIcon from 'components/Icon/FontIcon';

import SettingsBtn from 'components/Buttons/SettingsBtn';
import { SortDirection } from 'components/VirtualizedTableViewer';

import BrowseTable from '../../components/BrowseTable';
import { tableStyles } from '../../tableStyles';

const mapStateToProps = (state) => ({
  spaces: getSpaces(state)
});

@Radium
@injectIntl
export class AllSpacesView extends PureComponent {
  static propTypes = {
    spaces: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  getTableData = () => {
    const [ name, created, action ] = this.getTableColumns();
    return this.props.spaces.toList().sort((a, b) => b.get('isActivePin') - a.get('isActivePin')).map((item) => {
      const icon = <FontIcon type={item.get('iconClass')}/>;
      return {
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
          [created.key]: {
            node: () => moment(item.get('ctime')).format('MM/DD/YYYY'),
            value: new Date(item.get('ctime'))
          },
          [action.key]: {
            node: () => (
              <span className='action-wrap'>
                <SettingsBtn
                  routeParams={this.context.location.query}
                  menu={<AllSpacesMenu item={item}/>}
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
      { key: 'name', label: intl.formatMessage({ id: 'Common.Name' }), flexGrow: 1},
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

  render() {
    const { spaces, intl } = this.props;
    const numberOfSpaces = spaces ? spaces.size : 0;
    return (
      <Fragment>
        <SpacesLoader />
        <BrowseTable
          title={`${intl.formatMessage({ id: 'Space.AllSpaces' })} (${numberOfSpaces})`}
          buttons={
            <RestrictedArea rule={manageSpaceRule}>
              <LinkButton
                buttonStyle='primary'
                to={{ ...this.context.location, state: { modal: 'SpaceModal' }}}
                style={allSpacesAndAllSources.addButton}>
                {this.props.intl.formatMessage({ id: 'Space.AddSpace' })}
              </LinkButton>
            </RestrictedArea>
          }
          tableData={this.getTableData()}
          columns={this.getTableColumns()}
        />
      </Fragment>
    );
  }
}

export default connect(mapStateToProps)(AllSpacesView);
