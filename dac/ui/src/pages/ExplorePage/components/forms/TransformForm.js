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
import { Children, Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import deepEqual from 'deep-equal';
import { debounce } from 'lodash/function';
import { resetRecommendedTransforms } from 'actions/explore/recommended';
import ViewStateWrapper from 'components/ViewStateWrapper';
import DefaultWizardFooter from 'components/Wizards/components/DefaultWizardFooter';
import FormProgressWrapper from 'components/FormProgressWrapper';
import Message from 'components/Message';
import exploreUtils from 'utils/explore/exploreUtils';
import { content } from './TransformForm.less';

export function formWrapperProps(props) {
  return {
    transform: props.transform,
    onCancel: props.onCancel,
    error: props.error,
    submitting: props.submitting,
    dirty: props.dirty,
    valid: props.valid,
    values: props.values,
    handleSubmit: props.handleSubmit,
    dataset: props.dataset,
    viewState: props.viewState
  };
}

export class TransformForm extends Component {
  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    cards: PropTypes.instanceOf(Immutable.List),
    dirty: PropTypes.bool,
    valid: PropTypes.bool,
    values: PropTypes.object,
    error: PropTypes.object,
    submitting: PropTypes.bool,
    handleSubmit: PropTypes.func,
    onFormChange: PropTypes.func,
    onFormSubmit: PropTypes.func,
    loadTransformCardPreview: PropTypes.func,
    onValuesChange: PropTypes.func,
    onCancel: PropTypes.func,
    children: PropTypes.node,
    debounceDelay: PropTypes.number,
    dataset: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,

    resetRecommendedTransforms: PropTypes.func.isRequired
  };

  static defaultProps = {
    viewState: Immutable.fromJS({}),
    debounceDelay: 1000
  };

  constructor(props) {
    super(props);

    if (props.debounceDelay) {
      this.autoPeek = debounce(this.autoPeek, props.debounceDelay);
      this.updateCard = debounce(this.updateCard, props.debounceDelay);
    }
  }


  componentDidMount() {
    if (this.props.valid) {
      this.autoPeek(this.props.values);
      this.autoPeek.flush();
    }
    if (!exploreUtils.needsToLoadCardFormValuesFromServer(this.props.transform)) {
      this.props.values.cards.forEach((card, index) => {
        this.props.loadTransformCardPreview(index, card);
      });
    }
  }

  componentWillReceiveProps(nextProps) {
    const nextActiveCard = nextProps.values.activeCard;
    const nextCardValues = nextProps.values.cards && nextProps.values.cards[nextActiveCard];
    if (nextProps.valid && !deepEqual(this.props.values, nextProps.values)) {
      if (nextProps.onValuesChange) {
        nextProps.onValuesChange(nextProps.values, this.props.values);
      }

      this.autoPeek(nextProps.values);
      if (this.props.values.activeCard !== nextProps.values.activeCard && this.autoPeek.flush) {
        // trigger the first load (and full card changes) immediately (UX)
        this.autoPeek.flush();
      }
    }

    const isCardsChanged = nextProps.loadTransformCardPreview && nextProps.valid
      && this.props.values.activeCard === nextActiveCard
      // if we use deepEqual without strict mode, value = '' and value = 0 will be equal
      && !deepEqual(this.props.values.cards[this.props.values.activeCard], nextCardValues, { strict: true });
    if (isCardsChanged) {
      this.updateCard(nextProps);
    }
  }

  componentWillUnmount() {
    if (this.autoPeek.cancel) {
      this.autoPeek.cancel();
    }
    if (this.updateCard.cancel) {
      this.updateCard.cancel();
    }

    this.props.resetRecommendedTransforms();
  }

  autoPeek(previewValues) {
    return this.props.onFormSubmit(previewValues, 'autoPeek').catch(function(e) {
      if (!e || !e._error) {
        return Promise.reject(e);
      }
    }); // skip handleSubmit to not set submitting
  }

  updateCard(props) {
    const { activeCard } = props.values;
    this.props.loadTransformCardPreview(activeCard, props.values.cards[activeCard]);
  }

  render() {
    const { children, onFormSubmit, handleSubmit, error, viewState, submitting, style } = this.props;

    return (
      <form onSubmit={onFormSubmit ? handleSubmit(onFormSubmit) : null} ref='form' style={{ height: '100%' }}>
        {error && <Message messageType='error' message={error.message} messageId={error.id} />}
        <div style={{ position: 'relative' }}>
          {
            Children.count(children) > 0 && <ViewStateWrapper
              className={content}
              viewState={viewState}
              spinnerStyle={styles.spinner}
              hideChildrenWhenInProgress
              style={{ ...styles.formBody, ...style }}>
              <FormProgressWrapper submitting={submitting}>
                {children}
              </FormProgressWrapper>
            </ViewStateWrapper>
          }
          <DefaultWizardFooter {...this.props} />
        </div>
      </form>
    );
  }
}

export default connect(null, { resetRecommendedTransforms })(TransformForm);

const styles = {
  formBody: { minHeight: 214 },
  spinner: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    top: 0,
    height: 60,
    width: '100%'
  }
};
