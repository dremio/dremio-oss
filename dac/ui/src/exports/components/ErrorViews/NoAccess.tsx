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
import { Link } from "react-router";
import { NotFound } from "./NotFound";
import { Button } from "dremio-ui-lib/components";
import { useIntl } from "react-intl";

export const NoAccess = ({
  button: { to, text },
}: {
  button: { to: string; text: string };
}) => {
  const { formatMessage } = useIntl();
  return (
    <div style={{ height: "100%", display: "flex", alignItems: "center" }}>
      <NotFound
        title={formatMessage({ id: "403.NoAccess" })}
        img={false}
        action={
          //@ts-ignore
          <Button as={Link} variant="primary" to={to}>
            {formatMessage({ id: text })}
          </Button>
        }
      />
    </div>
  );
};
