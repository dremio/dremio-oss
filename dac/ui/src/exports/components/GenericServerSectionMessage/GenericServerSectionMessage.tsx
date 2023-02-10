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

import { HttpError } from "../../errors/HttpError";
import { SectionMessage } from "dremio-ui-lib/dist-esm";
import { HttpErrorSupportInfo } from "../SupportInfo/SupportInfo";

const DEFAULT_MESSAGE = "An unexpected error occured.";
const HELP_TEXT =
  "If this error persists please contact Dremio support and provide the following information:";

type Props = {
  error: HttpError;
  title?: string;
};

export const GenericServerSectionMessage = (props: Props): JSX.Element => {
  const { error, title, ...rest } = props;
  return (
    <SectionMessage
      title={title || DEFAULT_MESSAGE}
      appearance="danger"
      {...rest}
    >
      <p>{HELP_TEXT}</p>
      <p>{<HttpErrorSupportInfo error={error} />}</p>
    </SectionMessage>
  );
};
