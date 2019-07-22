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

import it.unimi.dsi.fastutil.ints.IntIterators;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the HLL++ {@link NormalRepresentation}.
 *
 * <p>IMPORTANT: Note that these tests were introduced later and currently don't cover the full
 * functionality of NormalRepresentation. All code paths are covered by the more abstract {@link
 * com.google.zetasketch.HyperLogLogPlusPlusTest}, however.
 */
// TODO: Remove warning above once unit test coverage is complete.
@RunWith(JUnit4.class)
public final class NormalRepresentationTest {

  /**
   * Verifies that the sparse precision is downgraded, even if no data conversion actually takes
   * place. This is important since we want the precision and sparse precision to match the lowest
   * seen values when the sketch is written to disk.
   */
  @Test
  public void addSparseValue_DowngradesSparsePrecision() {
    NormalRepresentation repr = create(10, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(10, 13);
    repr = repr.addSparseValue(sparseEncoding, 0b0000000000001);

    assertThat(repr.state.sparsePrecision).isEqualTo(13);
  }

  /**
   * Verifies that a sparse value with a higher precision is correctly downgraded to match the
   * representation.
   */
  @Test
  public void addSparseValue_HigherPrecision() {
    NormalRepresentation repr = create(10, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(11, 13);

    int sparseValue = 0b0000000000001;
    repr = repr.addSparseValue(sparseEncoding, sparseValue);

    // Assume that Encoding.Sparse has been sufficiently tested and is doing the right thing.
    byte[] expected = new byte[1 << 10];
    Encoding.Normal normalEncoding = new Encoding.Normal(10);
    int newIndex = sparseEncoding.decodeAndDowngradeNormalIndex(sparseValue, normalEncoding);
    byte newRhoW = sparseEncoding.decodeAndDowngradeNormalRhoW(sparseValue, normalEncoding);
    expected[newIndex] = newRhoW;
    assertThat(repr.state.data.toByteArray()).isEqualTo(expected);
  }

  /**
   * Verifies that a sparse value with a lower precision correctly causes the representation to
   * downgrade itself.
   */
  @Test
  public void addSparseValue_LowerPrecision() {
    NormalRepresentation repr = create(11, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(10, 13);

    int sparseValue = 0b0000000000001;
    repr = repr.addSparseValue(sparseEncoding, sparseValue);

    // Assume that Encoding.Sparse has been sufficiently tested and is doing the right thing.
    byte[] expected = new byte[1 << 10];
    Encoding.Normal normalEncoding = new Encoding.Normal(10);
    int newIndex = sparseEncoding.decodeAndDowngradeNormalIndex(sparseValue, normalEncoding);
    byte newRhoW = sparseEncoding.decodeAndDowngradeNormalRhoW(sparseValue, normalEncoding);
    expected[newIndex] = newRhoW;
    assertThat(repr.state.data.toByteArray()).isEqualTo(expected);
  }

  /**
   * Verifies that the sparse precision is downgraded, even if no data conversion actually takes
   * place. This is important since we want the precision and sparse precision to match the lowest
   * seen values when the sketch is written to disk.
   */
  @Test
  public void addSparseValues_DowngradesSparsePrecision() {
    NormalRepresentation repr = create(10, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(10, 13);
    repr = repr.addSparseValues(sparseEncoding, IntIterators.EMPTY_ITERATOR);

    assertThat(repr.state.sparsePrecision).isEqualTo(13);
  }

  /**
   * Verifies that sparse values with higher precision are correctly downgraded to match the
   * representation.
   */
  @Test
  public void addSparseValues_HigherPrecision() {
    NormalRepresentation repr = create(10, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(11, 13);

    int[] sparseValues = new int[] {0b0000000000001, 0b00000000011111};
    repr = repr.addSparseValues(sparseEncoding, IntIterators.wrap(sparseValues));

    // Assume that Encoding.Sparse has been sufficiently tested and is doing the right thing.
    byte[] expected = new byte[1 << 10];
    Encoding.Normal normalEncoding = new Encoding.Normal(10);
    for (int sparseValue : sparseValues) {
      int newIndex = sparseEncoding.decodeAndDowngradeNormalIndex(sparseValue, normalEncoding);
      byte newRhoW = sparseEncoding.decodeAndDowngradeNormalRhoW(sparseValue, normalEncoding);
      expected[newIndex] = newRhoW;
    }
    assertThat(repr.state.data.toByteArray()).isEqualTo(expected);
  }

  /**
   * Verifies that sparse values with lower precision correctly cause the representation to
   * downgrade itself.
   */
  @Test
  public void addSparseValues_LowerPrecision() {
    NormalRepresentation repr = create(11, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(10, 13);

    int[] sparseValues = new int[] {0b0000000000001, 0b0000000001001};
    repr = repr.addSparseValues(sparseEncoding, IntIterators.wrap(sparseValues));

    // Assume that Encoding.Sparse has been sufficiently tested and is doing the right thing.
    byte[] expected = new byte[1 << 10];
    for (int sparseValue : sparseValues) {
      int index = sparseEncoding.decodeNormalIndex(sparseValue);
      byte rhoW = sparseEncoding.decodeNormalRhoW(sparseValue);
      expected[index] = rhoW;
    }
    assertThat(repr.state.data.toByteArray()).isEqualTo(expected);
  }

  /** Verifies that null sparse values with a higher precision are ignored (b/118806044). */
  @Test
  public void addSparseValues_HigherPrecisionNull() {
    NormalRepresentation repr = create(10, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(11, 13);

    repr = repr.addSparseValues(sparseEncoding, null);

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    assertThat(repr.state.data).isNull();
  }

  /**
   * Verifies that null sparse values with a lower precision still cause the representation to
   * downgrade itself (b/118806044).
   */
  @Test
  public void addSparseValues_LowerPrecisionNull() {
    NormalRepresentation repr = create(11, 15);
    Encoding.Sparse sparseEncoding = new Encoding.Sparse(10, 13);

    repr = repr.addSparseValues(sparseEncoding, null);

    assertThat(repr.state.precision).isEqualTo(10);
    assertThat(repr.state.sparsePrecision).isEqualTo(13);
    assertThat(repr.state.data.toByteArray()).isEqualTo(new byte[1 << 10]);
  }

  /**
   * Verifies that the sparse precision is downgraded, even if no data conversion actually takes
   * place. This is important since we want the precision and sparse precision to match the lowest
   * seen values when the sketch is written to disk.
   */
  @Test
  public void merge_DowngradesSparsePrecision() throws Exception {
    NormalRepresentation a = create(10, 14);
    NormalRepresentation b = create(10, 15);

    b.merge(a);
    assertThat(b.state.sparsePrecision).isEqualTo(14);
  }

  /**
   * Tests that the HLL++ array of a normal representation is correctly updated when it receives a
   * normal representation of a higher precision.
   */
  @Test
  public void merge_NormalWithHigherPrecision() throws Exception {
    NormalRepresentation target = create(10, 15);
    target.addHash((0b0000000000L << 54) /* index */ | (0b001L << 51) /* rhoW = 2 + 1 = 3 */);
    target.addHash((0b0000000001L << 54) /* index */ | (0b111L << 51) /* rhoW = 0 + 1 = 1 */);

    NormalRepresentation source = create(11, 15);
    source.addHash((0b00000000000L << 53) /* index */ | (0b111L << 50) /* rhoW = 0 + 1 = 1 */);
    source.addHash((0b00000000010L << 53) /* index */ | (0b001L << 50) /* rhoW = 2 + 1 = 3 */);

    target = target.merge(source);

    byte[] expected = new byte[1 << 10];
    expected[0b0000000000] = 3; // preserved
    expected[0b0000000001] = 4; // updated (rhoW from target was 3, 4 after downgrade)
    assertThat(target.state.data.toByteArray()).isEqualTo(expected);
  }

  /**
   * Tests that the HLL++ array of a normal representation is correctly updated when it receives a
   * normal representation of a higher precision.
   *
   * <p>Merging from a lower precision is trickier since the receiver needs to downgrade themselves.
   */
  @Test
  public void merge_NormalWithLowerPrecision() throws Exception {
    NormalRepresentation source = create(10, 15);
    source.addHash((0b0000000000L << 54) /* index */ | (0b001L << 51) /* rhoW = 2 + 1 = 3 */);
    source.addHash((0b0000000001L << 54) /* index */ | (0b111L << 51) /* rhoW = 0 + 1 = 1 */);

    NormalRepresentation target = create(11, 15);
    target.addHash((0b00000000000L << 53) /* index */ | (0b111L << 50) /* rhoW = 0 + 1 = 1 */);
    target.addHash((0b00000000010L << 53) /* index */ | (0b001L << 50) /* rhoW = 2 + 1 = 3 */);

    target = target.merge(source);

    byte[] expected = new byte[1 << 10];
    expected[0b0000000000] = 3; // preserved
    expected[0b0000000001] = 4; // updated (rhoW from target was 3, 4 after downgrade)
    assertThat(target.state.data.toByteArray()).isEqualTo(expected);
    assertThat(target.state.precision).isEqualTo(10);
  }

  /** Creates a new NormalRepresentation with the given precisions. */
  private NormalRepresentation create(int normalPrecision, int sparsePrecision) {
    State state = new State();
    state.precision = normalPrecision;
    state.sparsePrecision = sparsePrecision;
    return new NormalRepresentation(state);
  }
}
