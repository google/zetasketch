/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasketch;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Immutable;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProtoOrBuilder;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.CustomValueType;
import java.util.Optional;

/** Represents the value type and associated operations that were used to create an aggregator. */
@AutoValue
@Immutable
public abstract class ValueType {

  public static final ValueType UNKNOWN = forNumber(0);

  /** Returns the number associated with this type. */
  public abstract int getNumber();

  /**
   * Returns the {@link DefaultOpsType.Id} for this type, if it represents one of those values.
   * Otherwise, returns {@link DefaultOpsType.Id#UNKNOWN}.
   */
  public final DefaultOpsType.Id asDefaultOpsType() {
    return Optional.ofNullable(DefaultOpsType.Id.forNumber(getNumber()))
        .orElse(DefaultOpsType.Id.UNKNOWN);
  }

  public final AggregatorStateProto.Builder copyToBuilder(
      AggregatorStateProto.Builder stateBuilder) {
    if (getNumber() == 0) {
      return stateBuilder.clearValueType();
    }
    return stateBuilder.setValueType(getNumber());
  }

  public static ValueType forStandardType(DefaultOpsType.Id opsType) {
    return new AutoValue_ValueType(opsType.getNumber());
  }

  public static ValueType forCustomType(CustomValueType.Id customType) {
    return new AutoValue_ValueType(customType.getNumber());
  }

  public static ValueType forNumber(int number) {
    return new AutoValue_ValueType(number);
  }

  public static ValueType forStateProto(AggregatorStateProtoOrBuilder proto) {
    return forNumber(proto.getValueType());
  }

  @Override
  public final String toString() {
    int number = getNumber();
    if (number == 0) {
      return "UNKNOWN";
    }

    final DefaultOpsType.Id defaultId = DefaultOpsType.Id.forNumber(number);
    if (defaultId != null) {
      return "DefaultOpsType.Id." + defaultId.name();
    }

    final CustomValueType.Id customId = CustomValueType.Id.forNumber(number);
    if (customId != null) {
      return "CustomValueType.Id." + customId.name();
    }

    return "<unnamed custom value type " + number + ">";
  }

  ValueType() {}
}

