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

import FontIcon from "@app/components/Icon/FontIcon";
import { useIntl } from "react-intl";
import { getIconByType } from "../../utils/utils";

type NamespaceIconProps = {
  type: string | null;
  elements?: string[];
};

export function NamespaceIcon({ type, elements }: NamespaceIconProps) {
  const intl = useIntl();
  const { type: iconType, id } = getIconByType(type, elements);
  return <FontIcon type={iconType} tooltip={intl.formatMessage({ id })} />;
}
