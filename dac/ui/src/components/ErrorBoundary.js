/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { Component, forwardRef, PureComponent } from 'react';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import { showAppError } from 'actions/prodError';
import sentryUtil from '@app/utils/sentryUtil';

const isProd = process.env.NODE_ENV === 'production';
const mapDispatchToProps = ({
  showAppError
});

@connect(null, mapDispatchToProps)
export class ErrorBoundary extends PureComponent {
  static propTypes = {
    children: PropTypes.any,
    // connected
    showAppError: PropTypes.func.isRequired
  };

  state = {
    hasError: false
  };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error) {
    if (!isProd) {
      debugger; // eslint-disable-line no-debugger
    }
    // This line must be here as in case DREMIO_RELEASE=true Sentry does not not catch a error
    sentryUtil.logException(error);
    this.props.showAppError(error);
  }

  render() {
    if (this.state.hasError) {
      // You can render any custom fallback UI
      return <h1>Something went wrong.</h1>;
    }

    return this.props.children;
  }
}

export const withErrorBoundary = ComponentToWrap => {
  class WithErrorBoundaryHOC extends Component {
    static propTypes = {
      forwardedRef: PropTypes.any
    };

    render() {
      return (
        <ErrorBoundary>
          <ComponentToWrap ref={this.props.forwardedRef} {...this.props} />
        </ErrorBoundary>
      );
    }
  }

  return forwardRef((props, ref) => <WithErrorBoundaryHOC forwardedRef={ref} {...props} />);
};
