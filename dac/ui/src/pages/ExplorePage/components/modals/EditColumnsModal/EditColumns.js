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
import { connect }   from 'react-redux';
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import uuid from 'uuid';
import Radium from 'radium';
import { FormattedMessage, injectIntl } from 'react-intl';

import ColumnsContainer from 'pages/ExplorePage/components/DnDColumns/ColumnsContainer';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import { getTableColumns } from 'selectors/explore';

import { formDescription, formLabel } from 'uiTheme/radium/typography';
import { form, formBody } from 'uiTheme/radium/forms';

import { updateTableColumns } from 'actions/explore/view';
import { resetGridColumnWidth } from 'actions/explore/ui';

import './EditColumnsModal.less';

// TODO: Implement visibility for subcolumns
// TODO: Implement sorting of subcolumns within parent column
@injectIntl
@Radium
@pureRender
export class EditColumns extends Component {
  static propTypes = {
    hide: PropTypes.func,
    columns: PropTypes.object.isRequired,
    updateTableColumns: PropTypes.func.isRequired,
    resetGridColumnWidth: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object
  }

  static defaultProps = {
    columns: new Immutable.List()
  }

  constructor(props) {
    super(props);
    this.state = {
      columns: props.columns,
      canDrag: true,
      resetWidth: false,
      resources:  this.mapResources(this.props.columns.toJS())
    };
  }

  onApply = () => {
    const columns = this.state.resetWidth
        ? this.state.columns.map(col => col.set('width', 0))
        : this.state.columns;
    this.props.updateTableColumns({columns, version: this.context.location.query.version})
    .then(() => this.props.hide());
  }

  onToggleVisible = (index) => {
    const hidden = this.state.columns.get(index).get('hidden');
    this.setState({
      columns: this.state.columns.setIn([index, 'hidden'], !hidden),
      canDrag: false
    });
  }

  allowDrag = () => {
    this.setState({
      canDrag: true
    });
  }

  /**
   * @description prepares resources/children for using in TreeView
   * @param  {array} childsItems
   * @return {array}             [{text: string, opened: bool, selected: bool,
   *                               path: string, children: arr, type: string}]
   */
  mapResources(childrenItems) {
    const resource = childrenItems.map((child) => {
      return {
        text: child.name.split('/')[0],
        opened: false,
        selected: false,
        path: child.name.toLowerCase(),
        children: [],
        arrow: child.parent || false,
        id: uuid.v4(),
        type: child.type
      };
    });
    return resource;
  }

  moveColumn = (dragIndex, hoverIndex) => {
    const { columns } = this.state;
    const dragColumn = columns.get(dragIndex);
    const updateColumn = columns.splice(dragIndex, 1).splice(hoverIndex, 0, dragColumn);
    this.setState({
      columns: updateColumn,
      resources: this.mapResources(updateColumn.toJS())
    });
  }

  preventClick(e) {
    e.stopPropagation();
  }

  render() {
    const { hide, intl } = this.props;

    return (
      <div onClick={this.preventClick} style={[form, style.base]}>
        <div style={[formBody, {flexGrow: 1}]}>
          <div style={[formDescription, style.element]}><FormattedMessage id='Dataset.FieldsVisibility'/></div>
          <div style={[formLabel, style.visibilityText]}><FormattedMessage id='Dataset.Visibility'/></div>
          <div style={[formLabel]}><FormattedMessage id='Dataset.Field'/></div>
          <ColumnsContainer
            columns={this.state.columns}
            moveColumn={this.moveColumn}
            allowDrag={this.allowDrag}
            canDrag={this.state.canDrag}
            onToggleVisible={this.onToggleVisible}
            resources={this.state.resources}/>
        </div>

        <ConfirmCancelFooter
          confirm={this.onApply}
          confirmText={intl.formatMessage({ id: 'Common.OK' })}
          cancelText={intl.formatMessage({ id: 'Common.Cancel' })}
          cancel={hide}
        />
      </div>
    );
  }
}

function mapStateToProps(state) {
  const location = state.routing.locationBeforeTransitions || {};
  return {
    columns: getTableColumns(state, location.query && location.query.version, location)
  };
}

export default connect(mapStateToProps, {
  resetGridColumnWidth,
  updateTableColumns
})(EditColumns);

const style = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden'
  },
  element: {
    marginBottom: 15,
    fontSize: '12px'
  },
  visibilityText: {
    float: 'left',
    marginRight: 25
  }
};
