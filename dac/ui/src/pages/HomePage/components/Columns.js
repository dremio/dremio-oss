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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import classNames from 'classnames';
import { getSidebarSize } from '@app/selectors/home';
import { sidebarColumn, homeRow, mainGridColumn } from './Columns.less';

const mapStateToProps = state => ({
  rightSidebarWidth: getSidebarSize(state)
});

const containerFactory = compClassName => {
  const Container = ({ children, style, className }) => {
    return (
      <div className={classNames(compClassName, className)} style={style}>
        {children}
      </div>
    );
  };
  Container.propTypes = {
    children: PropTypes.node,
    style: PropTypes.object,
    className: PropTypes.string
  };

  return Container;
};

export const Row = containerFactory(homeRow);

export const GridColumn = containerFactory(mainGridColumn);

export const SidebarColumnView = containerFactory(sidebarColumn);

export const SidebarColumn = connect(mapStateToProps)(({ rightSidebarWidth, style, ...rest }) => {
  const resultStyle = rightSidebarWidth ? {...style, width: rightSidebarWidth} : style;
  return <SidebarColumnView style={resultStyle} {...rest} />;
});
