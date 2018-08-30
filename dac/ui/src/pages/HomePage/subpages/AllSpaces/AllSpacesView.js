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
import { connect } from 'react-redux';
import moment from 'moment';
import { Link } from 'react-router';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import AllSpacesViewMixin from 'dyn-load/pages/HomePage/subpages/AllSpaces/AllSpacesViewMixin';
import { removeSpace } from 'actions/resources/spaces';
import { showConfirmationDialog } from 'actions/confirmation';

import * as allSpacesAndAllSources from 'uiTheme/radium/allSpacesAndAllSources';

import EllipsedText from 'components/EllipsedText';

import LinkButton from 'components/Buttons/LinkButton';
import ResourcePin from 'components/ResourcePin';
import { AllSpacesMenu } from 'components/Menus/HomePage/AllSpacesMenu';
import FontIcon from 'components/Icon/FontIcon';
import SettingsBtn from 'components/Buttons/SettingsBtn';
import { SortDirection } from 'components/VirtualizedTableViewer';

import BrowseTable from '../../components/BrowseTable';
import { tableStyles } from '../../tableStyles';

@Radium
@pureRender
@injectIntl
@AllSpacesViewMixin
export class AllSpacesView extends Component {
  static propTypes = {
    spaces: PropTypes.object,
    removeSpace: PropTypes.func,
    toggleActivePin: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func,
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
          [created.key]: {
            node: () => moment(item.get('ctime')).format('MM/DD/YYYY'),
            value: new Date(item.get('ctime'))
          },
          [action.key]: {
            node: () => (
              <span className='action-wrap'>
                <SettingsBtn
                  routeParams={this.context.location.query}
                  menu={<AllSpacesMenu space={item} removeSpace={this.removeSpace}/>}
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
      { key: 'name', title: intl.formatMessage({ id: 'Common.Name' }), flexGrow: 1},
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

  removeSpace = (spaceToRemove) => {
    const { intl } = this.props;
    this.props.showConfirmationDialog({
      text: intl.formatMessage({id: 'Space.WantToRemoveSpace'}),
      title: intl.formatMessage({id: 'Space.RemoveSpace'}),
      confirmText: intl.formatMessage({id: 'Common.Remove'}),
      confirm: () => this.props.removeSpace(spaceToRemove)
    });
  }

  renderAddButton() {
    return (
      <LinkButton
        buttonStyle='primary'
        to={{ ...this.context.location, state: { modal: 'SpaceModal' }}}
        style={allSpacesAndAllSources.addButton}>
        {this.props.intl.formatMessage({ id: 'Space.AddSpace' })}
      </LinkButton>
    );
  }

  render() {
    const { spaces, intl } = this.props;
    const numberOfSpaces = spaces ? spaces.size : 0;
    return (
      <BrowseTable
        title={`${intl.formatMessage({ id: 'Space.AllSpaces' })} (${numberOfSpaces})`}
        buttons={this.renderAddButton()}
        tableData={this.getTableData()}
        columns={this.getTableColumns()}
      />
    );
  }
}

export default connect(null, {
  removeSpace,
  showConfirmationDialog
})(AllSpacesView);
