/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class JsonNodeToLocalTimeCodec extends JsonNodeToTemporalCodec<LocalTime> {

  private final ZoneId timeZone;

  public JsonNodeToLocalTimeCodec(
      TemporalFormat parser, ZoneId timeZone, List<String> nullStrings) {
    super(LocalTimeCodec.instance, parser, nullStrings);
    this.timeZone = timeZone;
  }

  @Override
  public LocalTime externalToInternal(JsonNode s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toLocalTime(temporal, timeZone);
  }
}
