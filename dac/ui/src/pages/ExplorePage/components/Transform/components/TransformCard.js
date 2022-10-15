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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import clsx from "clsx";
import FontIcon from "components/Icon/FontIcon";
import CardFooter from "./CardFooter";
import * as classes from "./TransformCard.module.less";

export default class TransformCard extends Component {
  static propTypes = {
    front: PropTypes.node,
    back: PropTypes.node,
    card: PropTypes.instanceOf(Immutable.Map),
    active: PropTypes.bool,
    onClick: PropTypes.func,
  };

  constructor(props) {
    super(props);

    const hasExamples =
      props.card &&
      props.card.get("examplesList") &&
      props.card.get("examplesList").size;

    this.state = {
      editing: !hasExamples,
    };
  }

  componentWillReceiveProps(nextProps) {
    if (!nextProps.active) {
      this.setState({ editing: false });
    }
  }

  onToggleEdit = () => {
    this.setState({ editing: !this.state.editing });
  };

  render() {
    const { front, back, card, active, onClick } = this.props;
    const { editing } = this.state;
    return (
      <div
        className={clsx("transform-card", classes["base"], {
          [classes["active"]]: active,
          [classes["inactive"]]: !active,
        })}
        onClick={onClick}
      >
        {editing || !front ? back : front}
        {front && (
          <FontIcon
            type={editing ? "Return" : "EditSmall"}
            className={classes["toggler"]}
            onClick={this.onToggleEdit}
          />
        )}
        <CardFooter
          card={card}
          className={clsx(classes["base"], {
            [classes["active"]]: active,
            [classes["inactive"]]: !active,
          })}
        />
      </div>
    );
  }
}
