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
import it.unimi.dsi.fastutil.ints.IntIterator;
import javax.annotation.Nullable;

/**
 * Base class for the internal representations of HyperLogLog++.
 *
 * <p>HyperLogLog++ internally uses either a <em>sparse</em> or a <em>normal</em> representation,
 * depending on the current cardinality, in order to reduce the memory footprint. We encapsulate
 * these using a Strategy pattern in to {@link NormalRepresentation} and {@link
 * SparseRepresentation}.
 *
 * <p>Methods that modify the internal state return a Representation to be used for future calls.
 * This allows representations to undergo metamorphosis when they realize that they are no longer
 * applicable. Concretely, a sparse representation will upgrade itself to a normal representation
 * once it reaches a given size.
 */
public abstract class Representation {

  /** Value used to indicate that the sparse representation should not be used. */
  public static final int SPARSE_PRECISION_DISABLED = 0;

  /** Backing state of the HyperLogLog++ representation. */
  protected final State state;

  public Representation(State state) {
    this.state = state;
  }

  /**
   * Returns an appropriate {@link Representation} for the state encoded in the {@code state}. The
   * state must be a valid HyperLogLog++ state.
   */
  public static Representation fromState(State state) {
    Preconditions.checkArgument(
        !(state.hasSparseData() && state.sparsePrecision == SPARSE_PRECISION_DISABLED),
        "Must have a sparse precision when sparse data is set");

    if (state.hasData() || state.sparsePrecision == SPARSE_PRECISION_DISABLED) {
      return new NormalRepresentation(state);
    }

    return new SparseRepresentation(state);
  }

  /**
   * Adds the uniformly hashed value to the representation.
   *
   * @return the representation to be used going forward.
   */
  public abstract Representation addHash(long hash);

  /**
   * Adds the given sparse encoded value to this representation.
   *
   * @param encoding with which the sparse value was encoded
   * @param sparseValue sparse value to add
   * @return the representation to be used going forward
   */
  public abstract Representation addSparseValue(Encoding.Sparse encoding, int sparseValue);

  /**
   * Adds the given sorted list of sparse values to this representation.
   *
   * <p>If the given encoding has a precision that is smaller than the receiver's precision, a
   * downgraded representation will be returned. This is true even if the {@code sparseValues} are
   * {@code null} or empty;
   *
   * @param encoding with which the sparse value was encoded
   * @param sparseValues iterator from which to read the sparse values. It is important that this
   *     iterator returns the values in sorted order so that they can be efficiently merged.
   * @return the representation to be used going forward
   */
  public abstract Representation addSparseValues(
      Encoding.Sparse encoding, @Nullable IntIterator sparseValues);

  /**
   * Makes the representation as compact as possible.
   *
   * <p>Currently this only really applies to the sparse representation, where it means "merge the
   * temp list into the sparse list, and return a NormalRepresentation if the resulting sparse list
   * is too large".
   *
   * @return the representation to be used going forward.
   */
  public Representation compact() {
    return this;
  }

  /** Estimates the cardinality from the current representation. */
  public abstract long estimate();

  /**
   * Merges another representation into this one.
   *
   * <p>If the precision of the given representation is smaller than the receiver's precision, a
   * downgraded representation will be returned. This is true even if representation to be merged is
   * empty.
   *
   * @param other representation to be merged into this.
   * @return the representation to be used going forward
   */
  public final Representation merge(Representation other) {
    // We _could_ use double-dispatch here, but it makes the calls a bit harder to read and we
    // have all possible implementations of Representation in this package anyways.
    if (other instanceof NormalRepresentation) {
      return merge((NormalRepresentation) other);
    } else if (other instanceof SparseRepresentation) {
      return merge((SparseRepresentation) other);
    } else {
      throw new AssertionError("programming error, unknown representation: " + other);
    }
  }

  protected abstract Representation merge(NormalRepresentation other);

  protected abstract Representation merge(SparseRepresentation other);
}
