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
import { Link } from 'react-router';

import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { bodyWhite } from 'uiTheme/radium/typography';

import './HeaderLink.less';

@pureRender
export default class HeaderLink extends Component {
  static propTypes = {
    children: PropTypes.node,
    to: PropTypes.string.isRequired
  }

  static contextTypes = {
    location: PropTypes.object,
    routeParams: PropTypes.object,
    router: PropTypes.object
  }

  shouldLinkBeActive() { // NOTE: RadiumLink can still override this with `false`
    // small hack to prevent case when we have several active items in header for a root link
    const { tableId } = this.context.routeParams;
    if (this.props.to !== '/') return true;
    if (this.context.location.pathname === '/') return true;

    return this.context.location.pathname.match(/^\/(home|spaces?|sources?)(\/.*)?$/) && !tableId;
  }

  handleClick = (evt) => {
    if (isModifiedEvent(evt) || !isLeftClickEvent(evt)) return;

    const { router } = this.context;
    if (this.shouldLinkBeActive() && router.isActive(this.props.to)) {
      evt.preventDefault();
      router.replace({pathname: '/reload', state: {to: router.location}});
    }
  }

  render() {
    const {children, ...props} = this.props;

    const RadiumLink = Radium(Link);
    return (
      <div className='HeaderLink' style={styles.item}>
        <RadiumLink
          {...props}
          onClick={this.handleClick}
          style={styles.link}
          activeClassName={this.shouldLinkBeActive() ? 'active-link' : ''}
          activeStyle={this.shouldLinkBeActive() ? styles.activeLink : {}}
        >
          {children}
          <div className='header-link-underline'></div>
        </RadiumLink>
      </div>
    );
  }
}

const styles = {
  item: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '0 8px 0'
  },
  link: {
    ...bodyWhite,
    textDecoration: 'none',
    marginTop: 3,
    paddingBottom: 4,
    height: 17,
    display: 'block'
  },
  activeLink: {
    borderBottom: '2px solid rgba(255, 255, 255, 0.4)'
  }
};


function isLeftClickEvent(event) {
  return event.button === 0;
}

function isModifiedEvent(event) {
  return !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);
}
