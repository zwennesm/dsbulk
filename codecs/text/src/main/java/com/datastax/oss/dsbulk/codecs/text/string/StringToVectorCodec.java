/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.internal.core.type.codec.VectorCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StringToVectorCodec<SubtypeT extends Number>
    extends StringConvertingCodec<CqlVector<SubtypeT>> {

  private final ConvertingCodec<String, SubtypeT> stringCodec;

  public StringToVectorCodec(
      VectorCodec<SubtypeT> targetCodec,
      ConvertingCodec<String, SubtypeT> stringCodec,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.stringCodec = stringCodec;
  }

  @Override
  public CqlVector<SubtypeT> externalToInternal(String s) {

    // Logic below adapted from VectorCodec.parse() and CqlVector.from() but here we use the dsbulk
    // codecs for the subtype in order to enforce any additional behaviours
    //
    // Logically this probably makes more sense anyway.  It's the responsibilty of dsbulk to define
    // what sorts of formats it wants to support for vectors.  It can certainly re-use a
    // representation
    // known to work with vectors but it certainly isn't obligated to do so.
    if (s == null || s.isEmpty() || s.equalsIgnoreCase("NULL")) return null;
    ArrayList<SubtypeT> vals =
        Streams.stream(Splitter.on(", ").split(s.substring(1, s.length() - 1)))
            .map(this.stringCodec::externalToInternal)
            .collect(Collectors.toCollection(ArrayList::new));
    return CqlVector.newInstance(vals);
  }

  @Override
  public String internalToExternal(CqlVector<SubtypeT> cqlVector) {
    return this.internalCodec.format(cqlVector);
  }
}
