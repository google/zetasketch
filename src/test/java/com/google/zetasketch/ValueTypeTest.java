/*
 * Copyright 2019 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.CustomValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ValueTypeTest {

  @Test
  public void unknown() {
    assertThat(ValueType.UNKNOWN.getNumber()).isEqualTo(0);
    assertThat(ValueType.UNKNOWN.asDefaultOpsType()).isEqualTo(DefaultOpsType.Id.UNKNOWN);
    assertThat(ValueType.UNKNOWN.toString()).isEqualTo("UNKNOWN");

    assertThat(ValueType.forNumber(0)).isEqualTo(ValueType.UNKNOWN);
  }

  @Test
  public void forNumber_DefaultOpsTypeNumber() {
    ValueType valueType = ValueType.forNumber(DefaultOpsType.Id.INT32_VALUE);
    assertThat(valueType).isEqualTo(ValueType.forStandardType(DefaultOpsType.Id.INT32));
    assertThat(valueType.getNumber()).isEqualTo(DefaultOpsType.Id.INT32_VALUE);
    assertThat(valueType.asDefaultOpsType()).isEqualTo(DefaultOpsType.Id.INT32);
    assertThat(valueType.toString()).isEqualTo("DefaultOpsType.Id.INT32");
  }

  @Test
  public void forNumber_otherNumber() {
    ValueType valueType = ValueType.forNumber(12345);
    assertThat(valueType.getNumber()).isEqualTo(12345);
    assertThat(valueType.asDefaultOpsType()).isEqualTo(DefaultOpsType.Id.UNKNOWN);
    assertThat(valueType.toString()).isEqualTo("<unnamed custom value type 12345>");
  }

  @Test
  public void forStandardType() {
    assertThat(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING))
        .isEqualTo(ValueType.forNumber(DefaultOpsType.Id.BYTES_OR_UTF8_STRING_VALUE));
  }
}
