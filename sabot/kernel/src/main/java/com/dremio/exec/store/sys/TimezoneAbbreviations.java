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
package com.dremio.exec.store.sys;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;

/**
 * Class to hold TimezoneAbbr-offset mapping
 */
public class TimezoneAbbreviations {

  private static final Map<String, TimezoneAbbr> abbrOffsetMap;

  /**
   * Method to get offset for a timezone
   * Returns empty if the timezone is not in the mapping
   * @param abbr
   * @return
   */
  public static Optional<String> getOffset(String abbr) {
    String abbrUpper = abbr.toUpperCase();
    if(abbrOffsetMap.containsKey(abbrUpper)) {
      return Optional.of(abbrOffsetMap.get(abbrUpper).tz_offset);
    }
    return Optional.empty();
  }

  public static Iterator<Object> getIterator() {
    return Iterators.transform(abbrOffsetMap.values().iterator(), Functions.identity()); // transform so we don't have to typecast
  }

  /**
   * TimeZone Abbreviation POJO. This is the schema for sys.timezone_abbrevs
   */
  public static class TimezoneAbbr {
    public String timezone_abbrev;
    public String tz_offset;
    public boolean is_daylight_savings;

    public TimezoneAbbr(String abbr, String offset, boolean isDst) {
      this.timezone_abbrev = abbr;
      this.tz_offset = offset;
      this.is_daylight_savings = isDst;
    }
  }

  static {
    Map<String, TimezoneAbbr> tempMap = new HashMap<>();
    tempMap.put("ACDT", new TimezoneAbbr("ACDT", "+10:30", true));
    tempMap.put("ACSST", new TimezoneAbbr("ACSST", "+10:30", true));
    tempMap.put("ACST", new TimezoneAbbr("ACST", "+09:30", false));
    tempMap.put("ACT", new TimezoneAbbr("ACT", "-05:00", false));
    tempMap.put("ACWST", new TimezoneAbbr("ACWST", "+08:45", false));
    tempMap.put("ADT", new TimezoneAbbr("ADT", "-03:00", true));
    tempMap.put("AEDT", new TimezoneAbbr("AEDT", "+11:00", true));
    tempMap.put("AESST", new TimezoneAbbr("AESST", "+11:00", true));
    tempMap.put("AEST", new TimezoneAbbr("AEST", "+10:00", false));
    tempMap.put("AFT", new TimezoneAbbr("AFT", "+04:30", false));
    tempMap.put("AKDT", new TimezoneAbbr("AKDT", "-08:00", true));
    tempMap.put("AKST", new TimezoneAbbr("AKST", "-09:00", false));
    tempMap.put("ALMST", new TimezoneAbbr("ALMST", "+07:00", true));
    tempMap.put("ALMT", new TimezoneAbbr("ALMT", "+06:00", false));
    tempMap.put("AMST", new TimezoneAbbr("AMST", "+04:00", false));
    tempMap.put("AMT", new TimezoneAbbr("AMT", "-04:00", false));
    tempMap.put("ANAST", new TimezoneAbbr("ANAST", "+12:00", false));
    tempMap.put("ANAT", new TimezoneAbbr("ANAT", "+12:00", false));
    tempMap.put("ARST", new TimezoneAbbr("ARST", "-03:00", false));
    tempMap.put("ART", new TimezoneAbbr("ART", "-03:00", false));
    tempMap.put("AST", new TimezoneAbbr("AST", "-04:00", false));
    tempMap.put("AWSST", new TimezoneAbbr("AWSST", "+09:00", true));
    tempMap.put("AWST", new TimezoneAbbr("AWST", "+08:00", false));
    tempMap.put("AZOST", new TimezoneAbbr("AZOST", "+00:00", true));
    tempMap.put("AZOT", new TimezoneAbbr("AZOT", "-01:00", false));
    tempMap.put("AZST", new TimezoneAbbr("AZST", "+04:00", false));
    tempMap.put("AZT", new TimezoneAbbr("AZT", "+04:00", false));
    tempMap.put("BDST", new TimezoneAbbr("BDST", "+02:00", true));
    tempMap.put("BDT", new TimezoneAbbr("BDT", "+06:00", false));
    tempMap.put("BNT", new TimezoneAbbr("BNT", "+08:00", false));
    tempMap.put("BORT", new TimezoneAbbr("BORT", "+08:00", false));
    tempMap.put("BOT", new TimezoneAbbr("BOT", "-04:00", false));
    tempMap.put("BRA", new TimezoneAbbr("BRA", "-03:00", false));
    tempMap.put("BRST", new TimezoneAbbr("BRST", "-02:00", true));
    tempMap.put("BRT", new TimezoneAbbr("BRT", "-03:00", false));
    tempMap.put("BST", new TimezoneAbbr("BST", "+01:00", true));
    tempMap.put("BTT", new TimezoneAbbr("BTT", "+06:00", false));
    tempMap.put("CADT", new TimezoneAbbr("CADT", "+10:30", true));
    tempMap.put("CAST", new TimezoneAbbr("CAST", "+09:30", false));
    tempMap.put("CCT", new TimezoneAbbr("CCT", "+08:00", false));
    tempMap.put("CDT", new TimezoneAbbr("CDT", "-05:00", true));
    tempMap.put("CEST", new TimezoneAbbr("CEST", "+02:00", true));
    tempMap.put("CET", new TimezoneAbbr("CET", "+01:00", false));
    tempMap.put("CETDST", new TimezoneAbbr("CETDST", "+02:00", true));
    tempMap.put("CHADT", new TimezoneAbbr("CHADT", "+13:45", true));
    tempMap.put("CHAST", new TimezoneAbbr("CHAST", "+12:45", false));
    tempMap.put("CHUT", new TimezoneAbbr("CHUT", "+10:00", false));
    tempMap.put("CKT", new TimezoneAbbr("CKT", "-10:00", false));
    tempMap.put("CLST", new TimezoneAbbr("CLST", "-03:00", true));
    tempMap.put("CLT", new TimezoneAbbr("CLT", "-04:00", false));
    tempMap.put("COT", new TimezoneAbbr("COT", "-05:00", false));
    tempMap.put("CST", new TimezoneAbbr("CST", "-06:00", false));
    tempMap.put("CXT", new TimezoneAbbr("CXT", "+07:00", false));
    tempMap.put("DAVT", new TimezoneAbbr("DAVT", "+07:00", false));
    tempMap.put("DDUT", new TimezoneAbbr("DDUT", "+10:00", false));
    tempMap.put("EASST", new TimezoneAbbr("EASST", "-06:00", false));
    tempMap.put("EAST", new TimezoneAbbr("EAST", "-06:00", false));
    tempMap.put("EAT", new TimezoneAbbr("EAT", "+03:00", false));
    tempMap.put("EDT", new TimezoneAbbr("EDT", "-04:00", true));
    tempMap.put("EEST", new TimezoneAbbr("EEST", "+03:00", true));
    tempMap.put("EET", new TimezoneAbbr("EET", "+02:00", false));
    tempMap.put("EETDST", new TimezoneAbbr("EETDST", "+03:00", true));
    tempMap.put("EGST", new TimezoneAbbr("EGST", "+00:00", true));
    tempMap.put("EGT", new TimezoneAbbr("EGT", "-01:00", false));
    tempMap.put("EST", new TimezoneAbbr("EST", "-05:00", false));
    tempMap.put("FET", new TimezoneAbbr("FET", "+03:00", false));
    tempMap.put("FJST", new TimezoneAbbr("FJST", "+13:00", true));
    tempMap.put("FJT", new TimezoneAbbr("FJT", "+12:00", false));
    tempMap.put("FKST", new TimezoneAbbr("FKST", "-03:00", false));
    tempMap.put("FKT", new TimezoneAbbr("FKT", "-03:00", false));
    tempMap.put("FNST", new TimezoneAbbr("FNST", "-01:00", true));
    tempMap.put("FNT", new TimezoneAbbr("FNT", "-02:00", false));
    tempMap.put("GALT", new TimezoneAbbr("GALT", "-06:00", false));
    tempMap.put("GAMT", new TimezoneAbbr("GAMT", "-09:00", false));
    tempMap.put("GEST", new TimezoneAbbr("GEST", "+04:00", false));
    tempMap.put("GET", new TimezoneAbbr("GET", "+04:00", false));
    tempMap.put("GFT", new TimezoneAbbr("GFT", "-03:00", false));
    tempMap.put("GILT", new TimezoneAbbr("GILT", "+12:00", false));
    tempMap.put("GMT", new TimezoneAbbr("GMT", "+00:00", false));
    tempMap.put("GYT", new TimezoneAbbr("GYT", "-04:00", false));
    tempMap.put("HKT", new TimezoneAbbr("HKT", "+08:00", false));
    tempMap.put("HST", new TimezoneAbbr("HST", "-10:00", false));
    tempMap.put("ICT", new TimezoneAbbr("ICT", "+07:00", false));
    tempMap.put("IDT", new TimezoneAbbr("IDT", "+03:00", true));
    tempMap.put("IOT", new TimezoneAbbr("IOT", "+06:00", false));
    tempMap.put("IRKST", new TimezoneAbbr("IRKST", "+08:00", false));
    tempMap.put("IRKT", new TimezoneAbbr("IRKT", "+08:00", false));
    tempMap.put("IRT", new TimezoneAbbr("IRT", "+03:30", false));
    tempMap.put("IST", new TimezoneAbbr("IST", "+02:00", false));
    tempMap.put("JAYT", new TimezoneAbbr("JAYT", "+09:00", false));
    tempMap.put("JST", new TimezoneAbbr("JST", "+09:00", false));
    tempMap.put("KDT", new TimezoneAbbr("KDT", "+10:00", true));
    tempMap.put("KGST", new TimezoneAbbr("KGST", "+06:00", true));
    tempMap.put("KGT", new TimezoneAbbr("KGT", "+06:00", false));
    tempMap.put("KOST", new TimezoneAbbr("KOST", "+11:00", false));
    tempMap.put("KRAST", new TimezoneAbbr("KRAST", "+07:00", false));
    tempMap.put("KRAT", new TimezoneAbbr("KRAT", "+07:00", false));
    tempMap.put("KST", new TimezoneAbbr("KST", "+09:00", false));
    tempMap.put("LHDT", new TimezoneAbbr("LHDT", "+10:30", false));
    tempMap.put("LHST", new TimezoneAbbr("LHST", "+10:30", false));
    tempMap.put("LIGT", new TimezoneAbbr("LIGT", "+10:00", false));
    tempMap.put("LINT", new TimezoneAbbr("LINT", "+14:00", false));
    tempMap.put("LKT", new TimezoneAbbr("LKT", "+05:30", false));
    tempMap.put("MAGST", new TimezoneAbbr("MAGST", "+11:00", false));
    tempMap.put("MAGT", new TimezoneAbbr("MAGT", "+11:00", false));
    tempMap.put("MART", new TimezoneAbbr("MART", "-09:30", false));
    tempMap.put("MAWT", new TimezoneAbbr("MAWT", "+05:00", false));
    tempMap.put("MDT", new TimezoneAbbr("MDT", "-06:00", true));
    tempMap.put("MEST", new TimezoneAbbr("MEST", "+02:00", true));
    tempMap.put("MET", new TimezoneAbbr("MET", "+01:00", false));
    tempMap.put("METDST", new TimezoneAbbr("METDST", "+02:00", true));
    tempMap.put("MEZ", new TimezoneAbbr("MEZ", "+01:00", false));
    tempMap.put("MHT", new TimezoneAbbr("MHT", "+12:00", false));
    tempMap.put("MMT", new TimezoneAbbr("MMT", "+06:30", false));
    tempMap.put("MPT", new TimezoneAbbr("MPT", "+10:00", false));
    tempMap.put("MSD", new TimezoneAbbr("MSD", "+04:00", true));
    tempMap.put("MSK", new TimezoneAbbr("MSK", "+03:00", false));
    tempMap.put("MST", new TimezoneAbbr("MST", "-07:00", false));
    tempMap.put("MUST", new TimezoneAbbr("MUST", "+05:00", true));
    tempMap.put("MUT", new TimezoneAbbr("MUT", "+04:00", false));
    tempMap.put("MVT", new TimezoneAbbr("MVT", "+05:00", false));
    tempMap.put("MYT", new TimezoneAbbr("MYT", "+08:00", false));
    tempMap.put("NDT", new TimezoneAbbr("NDT", "-02:30", true));
    tempMap.put("NFT", new TimezoneAbbr("NFT", "-03:30", false));
    tempMap.put("NOVST", new TimezoneAbbr("NOVST", "+07:00", false));
    tempMap.put("NOVT", new TimezoneAbbr("NOVT", "+07:00", false));
    tempMap.put("NPT", new TimezoneAbbr("NPT", "+05:45", false));
    tempMap.put("NST", new TimezoneAbbr("NST", "-03:30", false));
    tempMap.put("NUT", new TimezoneAbbr("NUT", "-11:00", false));
    tempMap.put("NZDT", new TimezoneAbbr("NZDT", "+13:00", true));
    tempMap.put("NZST", new TimezoneAbbr("NZST", "+12:00", false));
    tempMap.put("NZT", new TimezoneAbbr("NZT", "+12:00", false));
    tempMap.put("OMSST", new TimezoneAbbr("OMSST", "+06:00", false));
    tempMap.put("OMST", new TimezoneAbbr("OMST", "+06:00", false));
    tempMap.put("PDT", new TimezoneAbbr("PDT", "-07:00", true));
    tempMap.put("PET", new TimezoneAbbr("PET", "-05:00", false));
    tempMap.put("PETST", new TimezoneAbbr("PETST", "+12:00", false));
    tempMap.put("PETT", new TimezoneAbbr("PETT", "+12:00", false));
    tempMap.put("PGT", new TimezoneAbbr("PGT", "+10:00", false));
    tempMap.put("PHOT", new TimezoneAbbr("PHOT", "+13:00", false));
    tempMap.put("PHT", new TimezoneAbbr("PHT", "+08:00", false));
    tempMap.put("PKST", new TimezoneAbbr("PKST", "+06:00", true));
    tempMap.put("PKT", new TimezoneAbbr("PKT", "+05:00", false));
    tempMap.put("PMDT", new TimezoneAbbr("PMDT", "-02:00", true));
    tempMap.put("PMST", new TimezoneAbbr("PMST", "-03:00", false));
    tempMap.put("PONT", new TimezoneAbbr("PONT", "+11:00", false));
    tempMap.put("PST", new TimezoneAbbr("PST", "-08:00", false));
    tempMap.put("PWT", new TimezoneAbbr("PWT", "+09:00", false));
    tempMap.put("PYST", new TimezoneAbbr("PYST", "-03:00", true));
    tempMap.put("PYT", new TimezoneAbbr("PYT", "-04:00", false));
    tempMap.put("RET", new TimezoneAbbr("RET", "+04:00", false));
    tempMap.put("SADT", new TimezoneAbbr("SADT", "+10:30", true));
    tempMap.put("SAST", new TimezoneAbbr("SAST", "+02:00", false));
    tempMap.put("SCT", new TimezoneAbbr("SCT", "+04:00", false));
    tempMap.put("SGT", new TimezoneAbbr("SGT", "+08:00", false));
    tempMap.put("TAHT", new TimezoneAbbr("TAHT", "-10:00", false));
    tempMap.put("TFT", new TimezoneAbbr("TFT", "+05:00", false));
    tempMap.put("TJT", new TimezoneAbbr("TJT", "+05:00", false));
    tempMap.put("TKT", new TimezoneAbbr("TKT", "+13:00", false));
    tempMap.put("TMT", new TimezoneAbbr("TMT", "+05:00", false));
    tempMap.put("TOT", new TimezoneAbbr("TOT", "+13:00", false));
    tempMap.put("TRUT", new TimezoneAbbr("TRUT", "+10:00", false));
    tempMap.put("TVT", new TimezoneAbbr("TVT", "+12:00", false));
    tempMap.put("UCT", new TimezoneAbbr("UCT", "+00:00", false));
    tempMap.put("ULAST", new TimezoneAbbr("ULAST", "+09:00", true));
    tempMap.put("ULAT", new TimezoneAbbr("ULAT", "+08:00", false));
    tempMap.put("UT", new TimezoneAbbr("UT", "+00:00", false));
    tempMap.put("UTC", new TimezoneAbbr("UTC", "+00:00", false));
    tempMap.put("UYST", new TimezoneAbbr("UYST", "-02:00", true));
    tempMap.put("UYT", new TimezoneAbbr("UYT", "-03:00", false));
    tempMap.put("UZST", new TimezoneAbbr("UZST", "+06:00", true));
    tempMap.put("UZT", new TimezoneAbbr("UZT", "+05:00", false));
    tempMap.put("VET", new TimezoneAbbr("VET", "-04:00", false));
    tempMap.put("VLAST", new TimezoneAbbr("VLAST", "+10:00", false));
    tempMap.put("VLAT", new TimezoneAbbr("VLAT", "+10:00", false));
    tempMap.put("VOLT", new TimezoneAbbr("VOLT", "+03:00", false));
    tempMap.put("VUT", new TimezoneAbbr("VUT", "+11:00", false));
    tempMap.put("WADT", new TimezoneAbbr("WADT", "+08:00", true));
    tempMap.put("WAKT", new TimezoneAbbr("WAKT", "+12:00", false));
    tempMap.put("WAST", new TimezoneAbbr("WAST", "+07:00", false));
    tempMap.put("WAT", new TimezoneAbbr("WAT", "+01:00", false));
    tempMap.put("WDT", new TimezoneAbbr("WDT", "+09:00", true));
    tempMap.put("WET", new TimezoneAbbr("WET", "+00:00", false));
    tempMap.put("WETDST", new TimezoneAbbr("WETDST", "+01:00", true));
    tempMap.put("WFT", new TimezoneAbbr("WFT", "+12:00", false));
    tempMap.put("WGST", new TimezoneAbbr("WGST", "-02:00", true));
    tempMap.put("WGT", new TimezoneAbbr("WGT", "-03:00", false));
    tempMap.put("XJT", new TimezoneAbbr("XJT", "+06:00", false));
    tempMap.put("YAKST", new TimezoneAbbr("YAKST", "+09:00", false));
    tempMap.put("YAKT", new TimezoneAbbr("YAKT", "+09:00", false));
    tempMap.put("YAPT", new TimezoneAbbr("YAPT", "+10:00", false));
    tempMap.put("YEKST", new TimezoneAbbr("YEKST", "+06:00", true));
    tempMap.put("YEKT", new TimezoneAbbr("YEKT", "+05:00", false));
    tempMap.put("Z", new TimezoneAbbr("Z", "+00:00", false));
    tempMap.put("ZULU", new TimezoneAbbr("ZULU", "+00:00", false));
    abbrOffsetMap = ImmutableSortedMap.copyOf(tempMap);
  }
}
