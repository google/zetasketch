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

import static com.google.common.truth.Truth.assertThat;

import com.google.zetasketch.internal.DifferenceDecoder;
import com.google.zetasketch.testing.IntegerBitIterableSubject;
import com.google.zetasketch.testing.IntegerBitSubject;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the HLL++ {@link SparseRepresentation}.
 *
 * <p>IMPORTANT: Note that these tests were introduced later and currently don't cover the full
 * functionality of SparseRepresentation. All code paths are covered by the more abstract {@link
 * com.google.zetasketch.HyperLogLogPlusPlusTest}, however.
 */
// TODO: Remove warning above once unit test coverage is complete.
@RunWith(JUnit4.class)
public final class SparseRepresentationTest {

  /**
   * Verifies that a sparse value with a higher precision is correctly downgraded to match the
   * representation.
   */
  @Test
  public void addSparseValue_HigherPrecision() {
    Representation repr = create(10, 13);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(11, 15);

    int sparseValue = 0b000000000011111;
    repr = repr.addSparseValue(sparseEncoding, sparseValue).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    IntegerBitIterableSubject.assertThat(new DifferenceDecoder(repr.state.sparseData))
        .isEqualTo(IntIterators.wrap(new int[] {0b000000000111}));
  }

  /**
   * Verifies that a sparse value with a lower precision correctly causes the representation to
   * downgrade itself.
   */
  @Test
  public void addSparseValue_LowerPrecision() {
    Representation repr = create(11, 15);
    Encoding.Sparse sourceEncoding = new Encoding.Sparse(10, 13);

    int sparseValue = 0b0000000000001;
    repr = repr.addSparseValue(sourceEncoding, sparseValue).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    IntegerBitSubject.assertThat(repr.state.sparseData.getNextVarInt()).isEqualTo(sparseValue);
  }

  /**
   * Verifies that sparse values with a higher precision are correctly downgraded to match the
   * representation.
   */
  @Test
  public void addSparseValues_HigherPrecision() {
    Representation repr = create(10, 13);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(11, 15);

    int[] sparseValues = new int[] {0b000000000000001, 0b000000000011111};
    repr = repr.addSparseValues(sparseEncoding, IntIterators.wrap(sparseValues)).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    IntegerBitIterableSubject.assertThat(new DifferenceDecoder(repr.state.sparseData))
        .isEqualTo(IntIterators.wrap(new int[] {0b000000000111, 0b10000000000000010}));
  }

  /** Verifies that null sparse values with a higher precision are ignored (b/118806044). */
  @Test
  public void addSparseValues_HigherPrecisionNull() {
    Representation repr = create(10, 13);
    Encoding.Sparse sourceEncoding = new Encoding.Sparse(11, 15);

    repr = repr.addSparseValues(sourceEncoding, null).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
  }

  /**
   * Verifies that sparse values with a lower precision correctly cause the representation to
   * downgrade itself.
   */
  @Test
  public void addSparseValues_LowerPrecision() {
    Representation repr = create(11, 15);
    Encoding.Sparse sourceEncoding = new Encoding.Sparse(10, 13);

    int[] sparseValues = new int[] {0b0000000000001, 0b0000000001111};
    repr = repr.addSparseValues(sourceEncoding, IntIterators.wrap(sparseValues)).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    IntegerBitIterableSubject.assertThat(new DifferenceDecoder(repr.state.sparseData))
        .isEqualTo(IntIterators.wrap(sparseValues));
  }

  /**
   * Verifies that null sparse values with a lower precision still cause the representation to
   * downgrade itself (b/118806044).
   */
  @Test
  public void addSparseValues_LowerPrecisionNull() {
    Representation repr = create(11, 15);
    Encoding.Sparse sourceEncoding = new Encoding.Sparse(10, 13);

    repr = repr.addSparseValues(sourceEncoding, null).compact();

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
  }

  private SparseRepresentation create(int normalPrecision, int sparsePrecision) {
    State state = new State();
    state.precision = normalPrecision;
    state.sparsePrecision = sparsePrecision;
    return new SparseRepresentation(state);
  }
}
