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
import { PureComponent, Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { Link } from 'react-router';
import { getEntityLinkUrl } from '@app/selectors/resources';
import { link } from 'uiTheme/radium/allSpacesAndAllSources';

const mapStateToProps = (state, { entityId }) => {

  return {
    linkTo: getEntityLinkUrl(state, entityId)
  };

};

/**
 * A link to a container entity for home page. Applicable for spaces, sources and home space
 */
export class EntityLink extends PureComponent {
  static propTypes = {
    //public api
    entityId: PropTypes.string,
    className: PropTypes.string,
    activeClassName: PropTypes.string,
    children: PropTypes.any,

    //connected
    linkTo: PropTypes.string.isRequired
  }

  render() {
    const { children,  className, activeClassName, linkTo } = this.props;

    return (
      <Link
        to={linkTo}
        activeClassName={activeClassName}
        style={link}
        className={className}
      >
        {children}
      </Link>
    );
  }
}

export default connect(mapStateToProps)(EntityLink);

export class EntityLinkProviderView extends Component {
  static propTypes = {
    entityId: PropTypes.string,
    /** (linkUrl: string): node */
    children: PropTypes.func,

    //connected
    linkTo: PropTypes.string.isRequired
  }

  render() {
    const { linkTo, children } = this.props;
    return children(linkTo);
  }
}

export const EntityLinkProvider = connect(mapStateToProps)(EntityLinkProviderView);
