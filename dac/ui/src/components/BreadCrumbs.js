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
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import Immutable from 'immutable';
import invariant from 'invariant';

import { splitFullPath } from 'utils/pathUtils';

class BreadCrumbs extends Component {
  static propTypes = {
    fullPath: PropTypes.instanceOf(Immutable.List).isRequired,
    pathname: PropTypes.string.isRequired,
    linkStyle: PropTypes.object,
    hideLastItem: PropTypes.bool
  };

  static defaultProps = {
    fullPath: Immutable.List(),
    hideLastItem: false
  };

  static formatFullPath(fullPath) {
    invariant(fullPath && fullPath.map, 'fullPath should be an Immutable.List');
    // if fullPath only contains one item, don't quote if it has a . in its name
    return fullPath.map((item) => fullPath.size > 1 && item.includes('.') ? `"${item}"` : item);
  }

  renderPath(els) {
    return (this.props.hideLastItem ? els.slice(0, -1) : els).map((el, index, arr) => {
      return index < arr.length - 1 ? <span key={index}>{el}.</span> : el; // add dots
    }, []);
  }

  render() {
    const { fullPath, pathname, linkStyle } = this.props;
    const formattedFullPath = BreadCrumbs.formatFullPath(fullPath);
    const els = getPathElements(formattedFullPath, pathname, linkStyle);
    const path = this.renderPath(els);
    return <span className='Breadcrumbs'>{path}</span>;
  }
}

export function getPathElements(fullPath, pathname, linkStyle) {
  const lastItem = fullPath.last();
  const fullPathWithoutLastItem = fullPath.slice(0, -1);
  const path = fullPathWithoutLastItem.reduce((prev, cur) => {
    return prev.concat(splitFullPath(cur));
  }, Immutable.List()).concat(lastItem);

  return path.map((item, index) => {
    if (index === path.count() - 1) {
      return <span key={item + index} style={linkStyle}>{item}</span>;
    }

    const href = getPartialPath(index, path, pathname);

    return <Link key={item + index} to={href} style={linkStyle}>{item}</Link>;
  }).toJS();
}



export function getPartialPath(index, fullPath, pathname) {
  // NOTE: This would be a lot easier if we had the parent container's url.
  // Instead we need to get the first part of the path (/space or /source) from the current location.pathname
  const pathnameParts = pathname.split('/');
  const fullPathRoot = fullPath.get(index);
  let partialPath;
  if (index === 0) {
    if (fullPathRoot[0] === '@') {
      partialPath = '/';
    } else {
      partialPath = `/${pathnameParts[1]}/${encodeURIComponent(fullPathRoot)}`;
    }
  } else {
    const encodedFullPath = fullPath.map((part) => encodeURIComponent(part));
    partialPath =
      `/${pathnameParts[1]}/${encodedFullPath.get(0)}/folder/${encodedFullPath.slice(1, index + 1).join('/')}`;
  }

  return partialPath;
}

export default BreadCrumbs;
