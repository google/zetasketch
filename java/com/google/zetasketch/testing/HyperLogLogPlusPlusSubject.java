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

import static com.google.common.truth.Fact.simpleFact;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.IntegerSubject;
import com.google.common.truth.LongSubject;
import com.google.common.truth.Subject;
import com.google.common.truth.Truth;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.HllplusUnique;
import com.google.zetasketch.HyperLogLogPlusPlus;
import com.google.zetasketch.ValueType;
import javax.annotation.Nullable;

/**
 * Test subject for {@link HyperLogLogPlusPlus} aggregators.
 *
 * <p>Example usage:
 *
 * <pre><code>
 *   import static com.google.zetasketch.testing.HyperLogLogPlusPlusSubject.assertThat;
 *   ...
 *   &#64;Test
 *   public void aggregatorIsTypeString() {
 *     HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();
 *     assertThat(aggregator).isTypeString();
 *   }
 * </code></pre>
 */
public class HyperLogLogPlusPlusSubject extends Subject {

  private static final Subject.Factory<HyperLogLogPlusPlusSubject, HyperLogLogPlusPlus<?>>
      HLL_SUBJECT_FACTORY = HyperLogLogPlusPlusSubject::new;

  /** Returns a test subject for the given target. */
  public static HyperLogLogPlusPlusSubject assertThat(@Nullable HyperLogLogPlusPlus<?> target) {
    return Truth.assertAbout(HLL_SUBJECT_FACTORY).that(target);
  }

  private final HyperLogLogPlusPlus<?> actual;

  HyperLogLogPlusPlusSubject(FailureMetadata failureMetadata, HyperLogLogPlusPlus<?> actual) {
    super(failureMetadata, actual);
    this.actual = actual;
  }

  /** Checks whether the aggregator is empty (i.e. no values have yet been added to it). */
  public void isEmpty() {
    if (actual.numValues() != 0) {
      failWithActual(simpleFact("expected to be empty"));
    }
  }

  /** Checks whether the aggregator is currently in normal mode. */
  public void isNormal() {
    if (!internalIsNormal()) {
      failWithActual(simpleFact("expected to be normal"));
    }
  }

  /** Checks whether the aggregator is currently in sparse mode. */
  public void isSparse() {
    if (internalIsNormal()) {
      failWithActual(simpleFact("expected to be sparse"));
    }
  }

  /** Checks whether the aggregator supports bytes. */
  public void isTypeBytes() {
    checkType(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));
  }

  /** Checks whether the aggregator supports longs. */
  public void isTypeLong() {
    checkType(ValueType.forStandardType(DefaultOpsType.Id.UINT64));
  }

  /** Checks whether the aggregator supports (standard UTF-8) strings. */
  public void isTypeString() {
    checkType(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));
  }

  /**
   * Returns a subject over {@link HyperLogLogPlusPlus#getNormalPrecision() normalPrecision} of the
   * aggregator.
   *
   * <p>Example usage:
   *
   * <pre><code>
   *   HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();
   *   assertThat(aggregator).normalPrecision().isEqualTo(15);
   * </code></pre>
   */
  public IntegerSubject normalPrecision() {
    return check("normalPrecision()").that(Integer.valueOf(actual.getNormalPrecision()));
  }

  /**
   * Returns a subject over {@link HyperLogLogPlusPlus#numValues() numValues} of the aggregator.
   *
   * <p>Example usage:
   *
   * <pre><code>
   *   HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();
   *   aggregator.add("foo");
   *   aggregator.add("foo");
   *   assertThat(aggregator).numValues().isEqualTo(2);
   * </code></pre>
   */
  public LongSubject numValues() {
    return check("numValues()").that(Long.valueOf(actual.numValues()));
  }

  /**
   * Returns a subject over {@link HyperLogLogPlusPlus#result() result} of the aggregator.
   *
   * <p>Example usage:
   *
   * <pre><code>
   *   HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();
   *   aggregator.add("foo");
   *   aggregator.add("foo");
   *   assertThat(aggregator).result().isEqualTo(1);
   * </code></pre>
   */
  public LongSubject result() {
    return check("result()").that(actual.result());
  }

  /**
   * Returns a subject over {@link HyperLogLogPlusPlus#getSparsePrecision() sparsePrecision} of the
   * aggregator.
   *
   * <p>Example usage:
   *
   * <pre><code>
   *   HyperLogLogPlusPlus<String> aggregator = new HyperLogLogPlusPlus.Builder().buildForStrings();
   *   assertThat(aggregator).sparsePrecision().isEqualTo(20);
   * </code></pre>
   */
  public IntegerSubject sparsePrecision() {
    return check("sparsePrecision()").that(Integer.valueOf(actual.getSparsePrecision()));
  }

  private void checkType(ValueType expected) {
    // A bit silly to have to serialize this to retrieve the value type, but it would be even more
    // silly to expose the internal value type on the public interface just for this test method.
    ValueType actual = ValueType.forStateProto(this.actual.serializeToProto());
    check("type()").that(actual).isEqualTo(expected);
  }

  private boolean internalIsNormal() {
    // If the aggregator has sparse mode disabled, it will always be in normal mode.
    if (actual.getSparsePrecision() == HyperLogLogPlusPlus.SPARSE_PRECISION_DISABLED) {
      return true;
    }

    // If the aggregator has normal data it has been upgraded to normal.
    if (actual
        .serializeToProto()
        .getExtension(HllplusUnique.hyperloglogplusUniqueState)
        .hasData()) {
      return true;
    }

    // In all other cases the sketch is sparse.
    return false;
  }
}
