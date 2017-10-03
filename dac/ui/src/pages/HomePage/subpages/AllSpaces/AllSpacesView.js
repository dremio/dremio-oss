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
import { connect }   from 'react-redux';
import moment from 'moment';
import { Link } from 'react-router';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';

import { removeSpace } from 'actions/resources/spaces';
import { showConfirmationDialog } from 'actions/confirmation';

import * as allSpacesAndAllSources from 'uiTheme/radium/allSpacesAndAllSources';

import EllipsedText from 'components/EllipsedText';

import LinkButton from 'components/Buttons/LinkButton';
import ResourcePin from 'components/ResourcePin';
import { AllSpacesMenu } from 'components/Menus/HomePage/AllSpacesMenu';
import FontIcon from 'components/Icon/FontIcon';
import SettingsBtn from 'components/Buttons/SettingsBtn';
import {SortDirection} from 'components/VirtualizedTableViewer';

import BrowseTable from '../../components/BrowseTable';
import { tableStyles } from '../../tableStyles';

@Radium
@pureRender
export class AllSpacesView extends Component {
  static propTypes = {
    spaces: PropTypes.object,
    removeSpace: PropTypes.func,
    toggleActivePin: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  getTableData = () => {
    const [ name, datasets, created, action ] = this.getTableColumns();
    return this.props.spaces.toList().sort((a, b) => b.get('isActivePin') - a.get('isActivePin')).map((item) => {
      const icon = <FontIcon type={item.get('iconClass')}/>;
      return {
        rowClassName: item.get('name'),
        data: {
          [name.title]: {
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
          [datasets.title]: {
            node: () => item.get('datasetCount'),
            value: item.get('datasetCount')
          },
          [created.title]: {
            node: () => moment(item.get('ctime')).format('MM/DD/YYYY'),
            value: new Date(item.get('ctime'))
          },
          [action.title]: {
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
    return [
      { title: la('name'), flexGrow: 1},
      { title: la('datasets'), style: tableStyles.datasetsColumn },
      { title: la('created') },
      {
        title: la('action'),
        style: tableStyles.actionColumn,
        disableSort: true,
        width: 60
      }
    ];
  }

  removeSpace = (spaceToRemove) => {
    this.props.showConfirmationDialog({
      text: la('Are you sure you want to remove the space?'),
      title: la('Remove Space'),
      confirmText: la('Remove'),
      confirm: () => this.props.removeSpace(spaceToRemove)
    });
  }

  renderAddButton() {
    return (
      <LinkButton
        buttonStyle='primary'
        to={{ ...this.context.location, state: { modal: 'SpaceModal' }}}
        style={allSpacesAndAllSources.addButton}>
        {la('Add Space')}
      </LinkButton>
    );
  }

  render() {
    const { spaces } = this.props;
    const numberOfSpaces = spaces ? spaces.size : 0;
    return (
      <BrowseTable
        title={`${la('All Spaces')} (${numberOfSpaces})`}
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
