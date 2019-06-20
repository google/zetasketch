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

package com.google.zetasketch.testing;

import static com.google.zetasketch.testing.HyperLogLogPlusPlusSubject.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.zetasketch.HyperLogLogPlusPlus;
import com.google.zetasketch.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link HyperLogLogPlusPlusSubject}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class HyperLogLogPlusPlusSubjectTest {

  private final HyperLogLogPlusPlus.Builder hllBuilder =
      new HyperLogLogPlusPlus.Builder().normalPrecision(10).sparsePrecision(25);
  private final HyperLogLogPlusPlus.Builder hllNoSparseBuilder =
      new HyperLogLogPlusPlus.Builder().normalPrecision(10).noSparseMode();

  @Test
  public void isEmpty() {
    HyperLogLogPlusPlus<String> agg = new HyperLogLogPlusPlus.Builder().buildForStrings();

    assertThat(agg).isEmpty();

    agg.add("foo");
    assertThrows(AssertionError.class, () -> assertThat(agg).isEmpty());
  }

  @Test
  public void isNormal() {
    assertThat(hllNoSparseBuilder.buildForStrings()).isNormal();

    assertThrows(
        AssertionError.class, () -> assertThat(hllBuilder.buildForStrings()).isNormal());
  }

  @Test
  public void isSparse() {
    assertThat(hllBuilder.buildForStrings()).isSparse();

    assertThrows(
        AssertionError.class, () -> assertThat(hllNoSparseBuilder.buildForStrings()).isSparse());
  }

  @Test
  public void isTypeBytes() {
    assertThat(hllBuilder.buildForBytes()).isTypeBytes();

    assertThrows(AssertionError.class, () -> assertThat(hllBuilder.buildForLongs()).isTypeBytes());
  }

  @Test
  public void isTypeLong() {
    assertThat(hllBuilder.buildForLongs()).isTypeLong();

    assertThrows(AssertionError.class, () -> assertThat(hllBuilder.buildForStrings()).isTypeLong());
  }

  @Test
  public void isTypeString() {
    assertThat(hllBuilder.buildForStrings()).isTypeString();

    assertThrows(AssertionError.class, () -> assertThat(hllBuilder.buildForLongs()).isTypeString());
  }

  @Test
  public void normalPrecision() {
    assertThat(hllBuilder.buildForStrings()).normalPrecision().isEqualTo(10);
  }

  @Test
  public void sparsePrecision() {
    assertThat(hllBuilder.buildForStrings()).sparsePrecision().isEqualTo(25);
  }

  @Test
  public void result() {
    HyperLogLogPlusPlus<Long> agg = hllBuilder.buildForLongs();
    agg.add(1L);
    agg.add(2L);
    agg.add(2L);
    assertThat(agg).result().isEqualTo(2);
  }

  @Test
  public void numValues() {
    HyperLogLogPlusPlus<Long> agg = hllBuilder.buildForLongs();
    agg.add(1L);
    agg.add(2L);
    agg.add(2L);
    assertThat(agg).numValues().isEqualTo(3);
  }
}
