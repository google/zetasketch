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

package com.google.zetasketch.internal.hllplus;

import com.google.common.base.Preconditions;
import com.google.zetasketch.internal.ByteSlice;
import it.unimi.dsi.fastutil.ints.IntIterator;
import javax.annotation.Nullable;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;

/**
 * Implementation of the normal HLL++ representation.
 *
 * <p>This class is <em>not</em> designed to be thread safe.
 */
public class NormalRepresentation extends Representation {

  /** The smallest normal precision supported by this representation. */
  public static final int MINIMUM_PRECISION = 10;

  /** The largest normal precision supported by this representation. */
  public static final int MAXIMUM_PRECISION = 24;

  /**
   * Utility class that encapsulates the encoding / decoding of individual HyperLogLog++ registers.
   */
  private final Encoding.Normal encoding;

  /**
   * Checks that the precision is valid for a normal representation, throwing an {@link
   * IllegalArgumentException} if not.
   */
  static void checkPrecision(int precision) throws IllegalArgumentException {
    Preconditions.checkArgument(
        MINIMUM_PRECISION <= precision && precision <= MAXIMUM_PRECISION,
        "Expected normal precision to be >= "
            + MINIMUM_PRECISION
            + " and <= "
            + MAXIMUM_PRECISION
            + " but was %s",
        precision);
  }

  public NormalRepresentation(State state) {
    super(state);
    checkPrecision(state.precision);

    if (state.hasData()) {
      Preconditions.checkArgument(
          state.data.remaining() == 1 << state.precision,
          "Expected data to consist of exactly %s bytes but got %s",
          1 << state.precision,
          state.data.remaining());
    }

    encoding = new Encoding.Normal(state.precision);
  }

  @Override
  public Representation addHash(long hash) {
    int idx = encoding.index(hash);
    byte rhoW = encoding.rhoW(hash);

    ensureData(state);

    // Update the rhoW at the given index if it is larger.
    state.data.putMax(idx, rhoW);
    return this;
  }

  @Override
  public NormalRepresentation addSparseValue(Encoding.Sparse encoding, int sparseValue) {
    NormalRepresentation repr = maybeDowngrade(encoding.normal(), encoding.sparsePrecision);

    // Add the sparse value to our backing array, downgrading it if necessary.
    byte[] data = getWriteableData(state);
    addSparseValueMaybeDowngrading(data, repr.encoding, sparseValue, encoding);

    return repr;
  }

  @Override
  public NormalRepresentation addSparseValues(
      Encoding.Sparse encoding, @Nullable IntIterator sparseValues) {
    NormalRepresentation repr = maybeDowngrade(encoding.normal(), encoding.sparsePrecision);

    if (sparseValues == null) {
      return repr;
    }

    // Add each sparse value to our backing array, downgrading it if necessary.
    byte[] data = getWriteableData(state);
    while (sparseValues.hasNext()) {
      addSparseValueMaybeDowngrading(data, repr.encoding, sparseValues.nextInt(), encoding);
    }

    return repr;
  }

  /**
   * Computes the cardinality estimate according to the algorithm in Figure 6 of the HLL++ paper
   * (https://goo.gl/pc916Z).
   */
  @Override
  public long estimate() {
    if (!state.hasData()) {
      return 0;
    }

    // Compute the summation component of the harmonic mean for the HLL++ algorithm while also
    // keeping track of the number of zeros in case we need to apply LinearCounting instead.
    int numZeros = 0;
    double sum = 0;

    // This loop is too specific to HLL++ to be moved into ByteSlice.
    byte[] data = state.data.array();
    int limit = state.data.arrayOffset() + state.data.limit();
    for (int i = state.data.arrayOffset(); i < limit; i++) {
      byte v = data[i];
      if (v == 0) {
        numZeros++;
      }

      // Compute sum += math.pow(2, -v) without actually performing a floating point exponent
      // computation (which is expensive). v can be at most 64 - precision + 1 and the minimum
      // precision is larger than 2 (see MINIMUM_PRECISION), so this left shift can not overflow.
      assert 0 <= v && v <= 65 - state.precision && state.precision >= MINIMUM_PRECISION;
      sum += 1.0 / (1L << v);
    }

    // Return the LinearCount for small cardinalities where, as explained in the HLL++ paper
    // (https://goo.gl/pc916Z), the results with LinearCount tend to be more accurate than with HLL.
    double m = 1 << state.precision;
    if (numZeros > 0) {
      double h = m * Math.log(m / numZeros);
      if (h <= Data.linearCountingThreshold(state.precision)) {
        return Math.round(h);
      }
    }

    // The "raw" estimate, designated by E in the HLL++ paper (https://goo.gl/pc916Z).
    double estimate = Data.alpha(state.precision) * m * m / sum;

    // Perform bias correction on small estimates. HyperLogLogPlusPlusData only contains bias
    // estimates for small cardinalities and returns 0 for anything else, so the "E < 5m" guard from
    // the HLL++ paper (https://goo.gl/pc916Z) is superfluous here.
    return Math.round(estimate - Data.estimateBias(estimate, state.precision));
  }

  @Override
  protected NormalRepresentation merge(NormalRepresentation other) {
    NormalRepresentation repr = maybeDowngrade(other.encoding, other.state.sparsePrecision);
    mergeNormalDataMaybeDowngrading(repr.state, repr.encoding, other.state.data, other.encoding);
    return repr;
  }

  @Override
  protected NormalRepresentation merge(SparseRepresentation other) {
    other.mergeInto(this);
    return this;
  }

  /**
   * Downgrades the current representation to match the precision of the normal and sparse encoding.
   * Returns <code>this</code> if no downgrade is necessary.
   */
  private NormalRepresentation maybeDowngrade(Encoding.Normal encoding, int sparsePrecision) {
    // If our precisions are less than the target precision, there is nothing that we need to do.
    // Values coming from the target sketch will be downgraded to our precision on add.
    if (state.precision <= encoding.precision && state.sparsePrecision <= sparsePrecision) {
      return this;
    }

    // Changes in normal precision require us to downgrade our current data.
    if (state.precision > encoding.precision) {
      ByteSlice sourceData = state.data;
      state.data = ByteSlice.allocate(1 << encoding.precision);
      state.precision = encoding.precision;
      mergeNormalDataMaybeDowngrading(state, encoding, sourceData, this.encoding);
    }

    // Changes in sparse precision require no data changes, but need to be reflected in the state.
    state.sparsePrecision = Math.min(state.sparsePrecision, sparsePrecision);
    return new NormalRepresentation(state);
  }

  /**
   * Merges a HyperLogLog++ sourceData array into a state, downgrading the values from the source
   * data if necessary. Note that this method requires the {@code targetEncoding} precision to be at
   * most the {@code sourceEncoding} precision and that it will not attempt to downgrade the state.
   */
  private static void mergeNormalDataMaybeDowngrading(
      State state,
      Encoding.Normal targetEncoding,
      @Nullable ByteSlice sourceData,
      Encoding.Normal sourceEncoding) {
    if (sourceData == null) {
      return;
    }

    // If the precisions are the same, then use a bulk update so this can be SSE-optimized by the
    // Java runtime.
    if (targetEncoding.precision == sourceEncoding.precision) {
      ensureData(state);
      state.data.putMax(0, sourceData);
      return;
    }

    // Merging from sketches from a higher precision can be a very frequent case so it's important
    // that this is implemented efficiently. Imagine a case where the vast majority of sketches have
    // a high precision but a single sketch has a lower precision. In this case, almost every merge
    // will be a precision downgrade.

    byte[] targetArray = getWriteableData(state);

    // Read from the source array directly. Since the source may be a read-only view into a byte
    // array, the offset may be > 0.
    byte[] sourceArray = sourceData.array();
    for (int oldIndex = 0; oldIndex < sourceData.limit(); oldIndex++) {
      byte oldRhoW = sourceArray[sourceData.arrayOffset() + oldIndex];

      int newIndex = sourceEncoding.downgradeIndex(oldIndex, targetEncoding);
      byte newRhoW = sourceEncoding.downgradeRhoW(oldIndex, oldRhoW, targetEncoding);

      // Update the rhoW at the given index if it is larger.
      if (targetArray[newIndex] < newRhoW) {
        targetArray[newIndex] = newRhoW;
      }
    }
  }

  /**
   * Adds a sparse value to a backing array, downgrading the values if necessary if the target
   * encoding has a lower precision than the source.
   */
  private static void addSparseValueMaybeDowngrading(
      byte[] data,
      Encoding.Normal targetEncoding,
      int sparseValue,
      Encoding.Sparse sourceEncoding) {
    int idx;
    byte rhoW;
    if (targetEncoding.precision < sourceEncoding.normalPrecision) {
      idx = sourceEncoding.decodeAndDowngradeNormalIndex(sparseValue, targetEncoding);
      rhoW = sourceEncoding.decodeAndDowngradeNormalRhoW(sparseValue, targetEncoding);
    } else {
      idx = sourceEncoding.decodeNormalIndex(sparseValue);
      rhoW = sourceEncoding.decodeNormalRhoW(sparseValue);
    }

    // Update the rhoW at the given index if it is larger.
    if (data[idx] < rhoW) {
      data[idx] = rhoW;
    }
  }

  /**
   * Ensures that the backing data is writeable, starting at offset 0 and returns it.
   *
   * <p>Using the backing array directly rather than going through ByteSlice avoids evaluating the
   * preconditions (writeable and valid index) within ByteSlice#putMax on each loop iteration. At
   * least at the time of this writing (2018 Q4) the hotspot compiler is unable to eliminate the
   * calls which cause a measurable 50% runtime overhead.
   */
  private static byte[] getWriteableData(State state) {
    ensureData(state);
    byte[] data = state.data.ensureWriteable().array();
    assert state.data != null && state.data.arrayOffset() == 0;
    return data;
  }

  /** Initializes the state's data if it is not already. */
  @EnsuresNonNull("#1.data")
  private static void ensureData(State state) {
    if (!state.hasData()) {
      state.data = ByteSlice.allocate(1 << state.precision);
    }
  }
}
