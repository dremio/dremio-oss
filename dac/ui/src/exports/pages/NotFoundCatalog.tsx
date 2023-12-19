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

import { ArcticSideNav } from "@app/exports/components/SideNav/ArcticSideNav";
import { NotFound } from "../components/ErrorViews/NotFound";
import { Button } from "dremio-ui-lib/components";
import { Link } from "react-router";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { ForbiddenError } from "dremio-ui-common/errors/ForbiddenError";
import narwhal403 from "dremio-ui-lib/icons/dremio/narwhal/narwhal-403.svg";
import * as PATHS from "@app/exports/paths";

export const NotFoundCatalog = ({ catalogErr }: any) => {
  const { t } = getIntlContext();
  const is403 = !!catalogErr && catalogErr instanceof ForbiddenError;
  return (
    <div className="page-content">
      <ArcticSideNav />
      <NotFound
        title={is403 ? t("Arctic.Catalog.Errors.403") : undefined}
        action={
          <Button as={Link} variant="primary" to={PATHS.arcticCatalogs()}>
            {t("Arctic.Catalog.Go.To.Catalogs")}
          </Button>
        }
        img={is403 ? <img src={narwhal403} alt="" /> : undefined}
      />
    </div>
  );
};
