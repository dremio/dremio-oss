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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import classNames from 'classnames';
import pureRender from 'pure-render-decorator';
import {FormattedMessage, injectIntl} from 'react-intl';

import PropTypes from 'prop-types';

import FinderNav from 'components/FinderNav';
import FinderNavItem from 'components/FinderNavItem';
import ViewStateWrapper from 'components/ViewStateWrapper';
import LinkButton from 'components/Buttons/LinkButton';
import SimpleButton from 'components/Buttons/SimpleButton';

import { formDescription } from 'uiTheme/radium/typography';
import { PALE_NAVY } from 'uiTheme/radium/colors';

import ApiUtils from 'utils/apiUtils/apiUtils';
import {createSampleSource, isSampleSource} from 'actions/resources/sources';

import Radium from 'radium';

import './LeftTree.less';

@injectIntl
@pureRender
@Radium
export class LeftTree extends Component {
  static contextTypes = {
    location: PropTypes.object,
    loggedInUser: PropTypes.object,
    router: PropTypes.object
  };

  static propTypes = {
    className: PropTypes.string,
    spaces: PropTypes.instanceOf(Immutable.List).isRequired,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    spacesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    toggleSpacePin: PropTypes.func,
    toggleSourcePin: PropTypes.func,
    createSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  state = {
    isAddingSampleSource: false
  };

  addSampleSource = () => {
    this.setState({isAddingSampleSource: true});
    return this.props.createSampleSource(this.props.sources, this.props.spaces).then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse('source', response);
        this.context.router.push(nextSource.getIn(['links', 'self']));
      }
      this.setState({isAddingSampleSource: false});
    });
  }

  getHomeObject() {
    return {
      name: this.context.loggedInUser.userName,
      links: {
        self: '/'
      },
      resourcePath: '/',
      iconClass: 'Home'
    };
  }

  getInitialSpacesContent() {
    const addHref = this.getAddSpaceHref();
    return this.props.spaces.size > 0 ? null : <div className='button-wrap'>
      <span style={formDescription}>
        <FormattedMessage id='Space.NoSpaces'/>
      </span>
      {addHref && <LinkButton
        buttonStyle='primary'
        data-qa={'add-spaces'}
        to={addHref}>
        <FormattedMessage id='Space.AddSpace'/>
      </LinkButton>}
    </div>;
  }

  getInitialSourcesContent() {
    const addHref = this.getAddSourceHref();
    const count = this.props.sources.size;

    const isEmpty = count === 0;
    const haveOnlySampleSource = !isEmpty && this.props.sources.toJS().every(isSampleSource);
    const haveNonSampleSource = this.props.sources.toJS().some(e => !isSampleSource(e));

    // situations...

    // - only sample source(s), user can't add: show nothing
    if (haveOnlySampleSource && !this.getCanAddSource()) return null;

    // - have a non-sample source: show nothing
    if (haveNonSampleSource) return null;

    // - no sources, user can add: show text and both buttons
    // - no sources, user can't add: show text
    // - only sample source(s), user can add: show text and add button
    return <div className='button-wrap'>
      <span style={formDescription}>
        {isEmpty
          ? <FormattedMessage id='Source.NoSources'/>
          : <FormattedMessage id='Source.AddOwnSource'/>}
      </span>
      {this.getCanAddSource() && isEmpty && <SimpleButton
        buttonStyle='primary'
        submitting={this.state.isAddingSampleSource}
        style={{padding: '0 12px'}}
        onClick={this.addSampleSource}>
        <FormattedMessage id='Source.AddSampleSource'/>
      </SimpleButton>}
      {this.getCanAddSource() && <LinkButton
        buttonStyle='primary'
        data-qa={'add-sources'}
        to={addHref}>
        <FormattedMessage id='Source.AddSource'/>
      </LinkButton>}
    </div>;
  }

  getAddSpaceHref() {
    return {...this.context.location, state: {modal: 'SpaceModal'}};
  }

  getCanAddSource() {
    return this.context.loggedInUser.admin;
  }

  getAddSourceHref() {
    return this.getCanAddSource() ?
      {...this.context.location, state: {modal: 'AddSourceModal'}} : undefined;
  }

  render() {
    const { className, spaces, sources, spacesViewState, sourcesViewState, intl } = this.props;
    const classes = classNames('left-tree', className);
    const homeItem = this.getHomeObject();

    return (
      <div className={classes} style={[styles.leftTreeHolder]}>
        <h3 style={[styles.headerViewer]}>
          <FormattedMessage id='Dataset.Datasets'/>
        </h3>
        <ul className='header-block' style={styles.homeWrapper}>
          <FinderNavItem item={homeItem} />
        </ul>
        <div className='left-tree-wrap' style={{
          ...styles.columnFlex,
          flex: '0 0 auto',
          overflow: 'hidden', // for FF (no worries, subsection will scroll)
          maxHeight: 'calc(61.8% - 50px)' // ~61.8% (golden ratio :P) of non-headers (doesn't need to be perfectly accurate)
        }}>
          <ViewStateWrapper viewState={spacesViewState} style={{...styles.columnFlex}}>
            <FinderNav
              navItems={spaces}
              title={intl.formatMessage({ id: 'Space.Spaces' })}
              toggleActivePin={this.props.toggleSpacePin}
              isInProgress={spacesViewState.get('isInProgress')}
              addHref={this.getAddSpaceHref()}
              listHref='/spaces/list'
              children={this.getInitialSpacesContent()}
            />
          </ViewStateWrapper>
        </div>
        <div className='left-tree-wrap' style={{
          ...styles.columnFlex,
          flex: '1 1 0%',
          overflow: 'hidden' // for FF (no worries, subsection will scroll)
        }}>
          <ViewStateWrapper viewState={sourcesViewState} style={{...styles.columnFlex}}>
            <FinderNav
              navItems={sources}
              title={intl.formatMessage({ id: 'Source.Sources' })}
              toggleActivePin={this.props.toggleSourcePin}
              isInProgress={sourcesViewState.get('isInProgress')}
              addHref={this.getAddSourceHref()}
              listHref='/sources/list'
              children={this.getInitialSourcesContent()}
            />
          </ViewStateWrapper>
        </div>
      </div>
    );
  }
}

export default connect(null, {createSampleSource})(LeftTree);

const styles = {
  leftTreeHolder: {
    width: '250px',
    flexShrink: '0',
    overflow: 'auto',
    borderRight: '1px solid rgba(0,0,0,.1)',
    display: 'flex',
    flexDirection: 'column',
    maxHeight: '100%'
  },
  homeWrapper: {
    flex: '0 0 auto',

    // this lines up the separator lines between the two panels:
    // (actually it's slightly off, but due to color differences/gestalt this actually looks more correct)
    paddingTop: 5,
    paddingBottom: 5
  },
  hItem: {
    display: 'block',
    width: '230px',
    height: '28px',
    padding: '2px 0',
    position: 'relative',
    cursor: 'pointer',
    borderRadius: '2px',
    ':hover': {
      background: '#fff5dc'
    }
  },
  userText: {
    margin: '5px 0 0 6px',
    display: 'inline-block'
  },
  fontIcon: {
    Container: {
      verticalAlign: 'top'
    }
  },
  headerViewer: {
    display: 'flex',
    alignItems: 'center',
    background: PALE_NAVY,
    height: 38,
    flex: '0 0 auto',
    padding: '0 10px'
  },
  columnFlex: { // we need this in a number of places to keep the DOM tree in flex mode
    display: 'flex',
    flexDirection: 'column'
  }
};
