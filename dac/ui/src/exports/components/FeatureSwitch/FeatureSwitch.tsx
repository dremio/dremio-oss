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

import { useFeatureFlag } from "../../providers/useFeatureFlag";
import { type Flag } from "../../flags/Flag.type";

type Props = {
  flag: Flag;
  renderEnabled: () => JSX.Element;
  renderDisabled?: () => JSX.Element;
  renderPending?: () => JSX.Element;
};

const nullRender = () => null;

/**
 * Conditionally renders children based on the provided feature flag
 */
export const FeatureSwitch = (props: Props): JSX.Element | null => {
  const {
    renderDisabled = nullRender,
    renderEnabled,
    renderPending = nullRender,
  } = props;
  const [result, loading] = useFeatureFlag(props.flag);

  if (loading) {
    return renderPending();
  }

  if (!result) {
    return renderDisabled();
  }

  return renderEnabled();
};
