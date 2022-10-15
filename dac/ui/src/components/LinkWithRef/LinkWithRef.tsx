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

/**
 * React router does not forward the ref. Ref is needed for Tooltip
 * Will probably have to remove this when updating react router
 */

import { forwardRef } from "react";
import { Link, type LinkProps } from "react-router";

const ReactRouterLink = Link as any;
const LinkWithRef = forwardRef<HTMLAnchorElement, LinkProps>((props, ref) => (
  <ReactRouterLink {...props} innerRef={ref} />
));
LinkWithRef.displayName = "LinkWithRef";

export default LinkWithRef;
