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
import { PageTypes as ExplorePageTypes } from 'pages/ExplorePage/pageTypes';
import { changePageTypeInUrl } from '@app/pages/ExplorePage/pageTypeUtils';
import { ENTITY_TYPES } from '@app/constants/Constants';

//This is a bit hacky, but it won't be necessary after server and client urls match

// full path is the . path used in sql

export function splitFullPath(fullPath, preserveQuoting) {
  const PATH_DELIMITER = '.';
  const QUOTE = '"';

  const result = [];
  let buffer = '';
  let isInQuote = false;

  function addBufferToResult() {
    // home name will have " around it which we want to remove for paths
    if (!preserveQuoting && buffer.length > 1 && buffer[0] === '"' && buffer[buffer.length - 1] === '"') {
      result.push(buffer.slice(1, -1));
    } else {
      result.push(buffer);
    }

    buffer = '';
  }

  for (let i = 0; i < fullPath.length; i++) {
    const char = fullPath.charAt(i);

    if (char === QUOTE) {
      if (isInQuote) {
        // check for double QUOTE, which means its a escaped value
        if (i < fullPath.length - 1 && fullPath[i + 1] === '"') {
          buffer += QUOTE;
          i++;
        } else {
          buffer += char;
          isInQuote = false;
        }
      } else {
        buffer += char;
        isInQuote = true;
      }
    } else if (char === PATH_DELIMITER) {
      if (isInQuote) {
        buffer += char;
      } else {
        // delimiter hit
        addBufferToResult();
      }
    } else {
      buffer += char;
    }
  }

  // TODO: what if we are still in a quote at this point?
  if (buffer.length) {
    addBufferToResult();
  }

  return result;
}

export function getEntityNameFromId(spaceId) {
  const splittedEntityId = spaceId.split('/');
  return splittedEntityId[splittedEntityId.length - 1];
}

export function isHomePage(pathname) {
  //TODO for some reasons if we open folder in home we will have spaces pathname, so we can't check correctly
  return pathname === '/home';
}

export function parseResourceId(pathname, username) {
  const parts = pathname.split('/');
  let resourceId = parts[2];
  if (parts.length === 2 || [ENTITY_TYPES.source, ENTITY_TYPES.space].indexOf(parts[1]) === -1) {
    resourceId = `@${username}`;
  }
  resourceId = `"${resourceId}"`;
  if (parts[3] === 'folder') {
    return [resourceId].concat(parts.slice(4).map(it => `"${it}"`)).join('.');
  }
  return resourceId;
}

export function getSourceNameFromResourceId(resourceId) {
  return resourceId.indexOf('.') > -1
    ? resourceId.slice(0, resourceId.indexOf('.'))
    : resourceId;
}

export function getEntityType(urlPath) {
  if (urlPath === '/') {
    return ENTITY_TYPES.home;
  }
  const pathParts = urlPath.split('/');
  if (pathParts.length < 4) {
    return pathParts[1];
  }
  return pathParts[3];
}

export function getRootEntityType(urlPath) {
  if (!urlPath) {
    return undefined;
  }
  if (urlPath === '/') {
    return ENTITY_TYPES.home;
  }
  const pathParts = urlPath.split('/');
  return pathParts[1];
}

export const RESERVED_WORDS = new Set((
  // copied from sabot/kernel/target/classes/sql-reserved-keywords.txt
  'ABS ALL ALLOCATE ALLOW ALTER AND ANY ARE ARRAY AS ASENSITIVE ASYMMETRIC AT ATOMIC AUTHORIZATION AVG BEGIN BETWEEN BIGINT BINARY BIT BLOB BOOLEAN BOTH BY CALL CALLED CARDINALITY CASCADED CASE CAST CEIL CEILING CHAR CHARACTER CHARACTER_LENGTH CHAR_LENGTH CHECK CLOB CLOSE COALESCE COLLATE COLLECT COLUMN COMMIT CONDITION CONNECT CONSTRAINT CONTAINS( CONVERT CORR CORRESPONDING COUNT COVAR_POP COVAR_SAMP CREATE CROSS CUBE CUME_DIST CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_DEFAULT_TRANSFORM_GROUP CURRENT_PATH CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME CURRENT_TIMESTAMP CURRENT_TRANSFORM_GROUP_FOR_TYPE CURRENT_USER CURSOR CYCLE DATABASES DATE DAY DEALLOCATE DEC DECIMAL DECLARE DEFAULT DELETE DENSE_RANK DEREF DESCRIBE DETERMINISTIC DISALLOW DISCONNECT DISTINCT DOUBLE DROP DYNAMIC EACH ELEMENT ELSE END END-EXEC ESCAPE EVERY EXCEPT EXEC EXECUTE EXISTS EXP EXPLAIN EXTEND EXTERNAL EXTRACT FALSE FETCH FILES FILTER FIRST_VALUE FLOAT FLOOR FOR FOREIGN FREE FROM FULL FUNCTION FUSION GET GLOBAL GRANT GROUP GROUPING HAVING HOLD HOUR IDENTITY IF IMPORT IN INDICATOR INNER INOUT INSENSITIVE INSERT INT INTEGER INTERSECT INTERSECTION INTERVAL INTO IS JOIN LANGUAGE LARGE LAST_VALUE LATERAL LEADING LEFT LIKE LIMIT LN LOCAL LOCALTIME LOCALTIMESTAMP LOWER MATCH MAX MEMBER MERGE METADATA METHOD MIN MINUS MINUTE MOD MODIFIES MODULE MONTH MULTISET NATIONAL NATURAL NCHAR NCLOB NEW NEXT NO NONE NORMALIZE NOT NULL NULLIF NUMERIC OCTET_LENGTH OF OFFSET OLD ON ONLY OPEN OR ORDER OUT OUTER OVER OVERLAPS OVERLAY PARAMETER PARTITION PERCENTILE_CONT PERCENTILE_DISC PERCENT_RANK POSITION POWER PRECISION PREPARE PRIMARY PROCEDURE RANGE RANK READS REAL RECURSIVE REF REFERENCES REFERENCING REFRESH REGR_AVGX REGR_AVGY REGR_COUNT REGR_INTERCEPT REGR_R2 REGR_SLOPE REGR_SXX REGR_SXY REGR_SYY RELEASE RESET RESULT RETURN RETURNS REVOKE RIGHT ROLLBACK ROLLUP ROW ROWS ROW_NUMBER SAVEPOINT SCHEMAS SCOPE SCROLL SEARCH SECOND SELECT SENSITIVE SESSION_USER SET SHOW SIMILAR SMALLINT SOME SPECIFIC SPECIFICTYPE SQL SQLEXCEPTION SQLSTATE SQLWARNING SQRT START STATIC STDDEV_POP STDDEV_SAMP STREAM SUBMULTISET SUBSTRING SUM SYMMETRIC SYSTEM SYSTEM_USER TABLE TABLES TABLESAMPLE THEN TIME TIMESTAMP TIMEZONE_HOUR TIMEZONE_MINUTE TINYINT TO TRAILING TRANSLATE TRANSLATION TREAT TRIGGER TRIM TRUE UESCAPE UNION UNIQUE UNKNOWN UNNEST UPDATE UPPER UPSERT USE USER USING VALUE VALUES VARBINARY VARCHAR VARYING VAR_POP VAR_SAMP WHEN WHENEVER WHERE WIDTH_BUCKET WINDOW WITH WITHIN WITHOUT YEAR'

  // TYPES:
  // list from https://github.com/dremio/dremio/blob/master/sabot/logical/src/main/java/com/dremio/common/expression/CompleteType.java
  // minus null, late, object (internal types)
  + ' ' + 'VARBINARY BIT DATE FLOAT DOUBLE INTERVAL_DAY_SECONDS INTERVAL_YEAR_MONTHS INT BIGINT TIME TIMESTAMP VARCHAR'
  // plus list, map, struct
  + ' ' + 'LIST MAP STRUCT'

  // plus text/x-sqlite list from codemirror because we don't have a list of type aliases
  //    https://github.com/codemirror/CodeMirror/blob/master/mode/sql/sql.js
  //    Portions: CodeMirror, copyright (c) by Marijn Haverbeke and others
  //    Distributed under an MIT license: http://codemirror.net/LICENSE
  + ' ' + 'BOOL BOOLEAN BIT BLOB DECIMAL DOUBLE FLOAT LONG LONGBLOB LONGTEXT MEDIUM MEDIUMBLOB MEDIUMINT MEDIUMTEXT TIME TIMESTAMP TINYBLOB TINYINT TINYTEXT TEXT CLOB BIGINT INT INT2 INT8 INTEGER FLOAT DOUBLE CHAR VARCHAR DATE DATETIME YEAR UNSIGNED SIGNED NUMERIC REAL'

  // todo: reconcile with constants/DataTypes.js, factor into independant list
).split(' '));

export function constructFullPath(pathParts, preventQuoted, shouldEncode) {
  if (!pathParts) {
    return undefined;
  }
  const quotedPathParts = pathParts.map((part) => {
    let encodedPart;
    if (preventQuoted || part.match(/^[A-Z][A-Z0-9]*$/i) && !RESERVED_WORDS.has(part.toUpperCase())) {
      encodedPart = part;
    } else {
      encodedPart = '"' + part.replace(/"/g, '""') + '"';
    }
    return shouldEncode ? encodeURIComponent(encodedPart) : encodedPart;
  });
  return quotedPathParts.join('.');
}

export function constructFullPathAndEncode(pathParts) {
  return constructFullPath(pathParts, false, true);
}

export function constructResourcePath(fullPath, type = 'dataset') {
  return `/${type}/${fullPath}`;
}

/**
 * Returns a path to a home space as a **string** if use creates a new vds or {@see fullPath} as
 * a **string**
 * @param {string[]} fullPath - current path
 * @param {string} datasetType
 * @param {string} username - current user name
 * @returns {string} - current entity path in dremio namespace
 */
export function getInitialResourceLocation(fullPath, datasetType, username) {
  return fullPath && fullPath[0] !== 'tmp' &&
         (datasetType === 'VIRTUAL_DATASET' || datasetType === 'PHYSICAL_DATASET_HOME_FILE')
         ? constructFullPath(fullPath, false)
         : `"@${username}"`;
}

export function getFullPathListFromEntity(entity) {
  return entity.get('fullPathList') || [];
}

export function getUniqueName(name, isUniqueFunc) {
  if (isUniqueFunc(name)) {
    return name;
  }

  // todo: loc
  let count = 1; // start at 1
  while (!isUniqueFunc(`${name} (${count})`)) {
    count++;
  }

  return `${name} (${count})`;
}

export function getRouteParamsFromLocation(location) {
  // this logic works only for Explore page
  let pathname = location && location.pathname;
  if (pathname) {
    try {
      pathname = decodeURI(pathname);
    } catch (e) { // if pathname is malformed, ignore it
      pathname = null;
    }
  }
  return {
    resourceId: (pathname) ? decodeURIComponent(pathname.split('/')[2]) : '',
    tableId: (pathname) ? decodeURIComponent(pathname.split('/')[3]) : ''
  };
}

export function navigateToExploreDefaultIfNecessary(pageType, location, router) {
  if (pageType !== ExplorePageTypes.default) {
    router.push({
      ...location,
      pathname: changePageTypeInUrl(location.pathname, ExplorePageTypes.default)
    });
  }
}
