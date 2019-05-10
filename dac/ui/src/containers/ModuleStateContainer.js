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
import { connect } from 'react-redux';
import { isModuleInitialized } from '@app/reducers';
import { initModuleState, resetModuleState } from '@app/actions/modulesState';

const mapStateToProps = (state, /* ownProps */ {
  moduleKey
}) => ({
  isStateInitialized: isModuleInitialized(state, moduleKey)
});

const mapDispatchToProps = {
  initState: initModuleState,
  resetState: resetModuleState
};

export class ModuleStateView extends Component {
  static propTypes = {
    moduleKey: PropTypes.string.isRequired, // todo create a list of available modules keys
    isStateInitialized: PropTypes.bool.isRequired, // dynamic state is initialized. [Connected property]
    reducer: PropTypes.func.isRequired, // (state, action) => newState - a reducer that would be used for module state
    initState: PropTypes.func.isRequired, // (moduleKey, reducer) => void
    resetState: PropTypes.func.isRequired, // (moduleKey) => void
    shouldResetState: PropTypes.func, // (prevProps, nextProps) => bool
    children: PropTypes.node
  };

  static defaultProps = {
    shouldResetState: (prevProps, nextProps) => {
      return nextProps.moduleKey !== prevProps.moduleKey ||
        nextProps.reducer !== prevProps.reducer;
    }
  };

  componentDidMount() {
    this.initState();
  }

  componentDidUpdate(prevProps) {
    if (this.props.shouldResetState(prevProps, this.props)) {
      this.resetState(prevProps); // reset previous state
      this.initState();
    }
  }

  componentWillUnmount() {
    this.resetState(this.props);
  }

  initState() {
    const {
      moduleKey,
      reducer,
      initState
    } = this.props;

    initState(moduleKey, reducer);
  }

  resetState(props) {
    const {
      moduleKey,
      resetState
    } = props;

    resetState(moduleKey);
  }

  render() {
    const {
      isStateInitialized,
      children
    } = this.props;

    return isStateInitialized ? children : null;
  }
}


const ModuleStateContainer = connect(mapStateToProps, mapDispatchToProps)(ModuleStateView);
export const moduleStateHOC = (moduleKey, reducer) => ComponentToWrap => {
  return class ModuleStateHOC extends Component {
    render() {
      return (
        <ModuleStateContainer moduleKey={moduleKey} reducer={reducer}>
          <ComponentToWrap {...this.props} />
        </ModuleStateContainer>
      );
    }
  };
};
