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
import com.google.zetasketch.IncompatiblePrecisionException;
import com.google.zetasketch.internal.DifferenceDecoder;
import com.google.zetasketch.internal.DifferenceEncoder;
import com.google.zetasketch.internal.GrowingByteSlice;
import com.google.zetasketch.internal.MergedIntIterator;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntHash.Strategy;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntOpenCustomHashSet;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Implementation of the sparse HLL++ representation.
 *
 * <p>This class is <em>not</em> designed to be thread safe.
 */
public class SparseRepresentation extends Representation {

  /** The largest sparse precision supported by this implementation. */
  public static final int MAXIMUM_SPARSE_PRECISION = 25;

  /**
   * The maximum amount of encoded sparse data, relative to the normal representation size, before
   * we upgrade to normal.
   *
   * <p>Note that, while some implementations also take into consideration the size of the temporary
   * (in-memory) {@link #buffer}, we define this field only relative to the normal representation
   * size as the golden tests verify that representations are upgraded consistently (relative to the
   * on-disk size). This allows us to fine-tune the size of the temporary {@link #buffer}
   * independently (e.g. improving runtime performance while trading off for peak memory usage).
   */
  private static final float MAXIMUM_SPARSE_DATA_FRACTION = 0.75f;

  /**
   * The maximum amount of elements that the temporary {@link #buffer} may contain before it is
   * flushed, relative to the number of bytes that the data in the normal representation would
   * require.
   *
   * <p>The thinking for this is as follows: If the number of bytes that the normal representation
   * would occupy is <em>m</em>, then the maximum number of bytes that the encoded sparse data can
   * occupy is <em>0.75m</em> (see {@link #MAXIMUM_SPARSE_DATA_FRACTION} above). This leaves <em>
   * 0.25m = m&middot;2<sup>-2</sup></em> bytes of memory that the temporary buffer can use before
   * the overall in-memory footprint of the sparse representation exceeds that of the normal
   * representation. Since each element in the buffer requires 4 bytes (32-bit integers), we can at
   * most keep <em>m&middot;2<sup>-2</sup>/4 = m&middot;2<sup>-4</sup></em> elements before we
   * exceed the in-memory footprint of the normal representation data.
   *
   * <p>Now the problem is that writing and reading the difference encoded data is CPU expensive (it
   * is by far the limiting factor for sparse adds and merges) so there is a tradeoff between the
   * memory footprint and the CPU cost.
   * For this reason, we add a correction factor that allows the sparse representation to use a bit
   * more memory and thereby greatly increases the speed of adds and merges.
   *
   * <p> A value of <em>4</em> was chosen in consistency with a legacy HLL++ implementation but this
   * is something to be evaluated critically.
   *
   * <p>This results in a final elements to bytes ratio of <em>4&middot;m&middot;2<sup>-2</sup> =
   * m&middot;2<sup>-2</sup></em>. This means that the sparse representation can (in the worst case)
   * use 1.75x the amount of RAM than the normal representation would. It will always use less than
   * {@value #MAXIMUM_SPARSE_DATA_FRACTION} times the amount of space on disk, however.
   */
  private static final float MAXIMUM_BUFFER_ELEMENTS_FRACTION = 1 - MAXIMUM_SPARSE_DATA_FRACTION;

  /**
   * The maximum number of bytes that the {@link State#sparseData} may contain before we upgrade to
   * normal. See {@link #MAXIMUM_SPARSE_DATA_FRACTION} for more details.
   */
  private final int maxSparseDataBytes;

  /**
   * The maximum number of elements that the {@link #buffer} may contain before it is flushed into
   * the sparse {@link State#sparseData} representation. See {@link
   * #MAXIMUM_BUFFER_ELEMENTS_FRACTION} for details on how this is computed.
   */
  private final int maxBufferElements;

  /** Helper object for encoding and decoding individual sparse values. */
  final Encoding.Sparse encoding;

  /**
   * A buffer of integers which should be merged into the difference encoded {@link
   * State#sparseData}. The sparse representation in {@link State#sparseData} is more space
   * efficient but also slower to read and write to, so this buffer allows us to quickly return
   * when adding new values to the aggregator.
   */
  private final IntCollection buffer;

  /**
   * Reference to a previous data buffer used for {@link State#sparseData}. During {@link #buffer}
   * flushes, the previous data will be merged with the {@link #buffer} and replaced with the
   * merged data. Keeping a reference to the old data buffer allows us to reuse it and avoid
   * expensive byte array allocations when flushes happen frequently, such as during chains of adds
   * or merges.
   */
  private WeakReference<GrowingByteSlice> recycledData = new WeakReference<>(null);

  public SparseRepresentation(State state) {
    super(state);
    checkPrecision(state.precision, state.sparsePrecision);

    this.encoding = new Encoding.Sparse(state.precision, state.sparsePrecision);

    // Compute size limits for the encoded sparse data and temporary buffer relative to what the
    // normal representation would require (which is 2^p bytes).
    assert state.precision < 31;
    int m = 1 << state.precision;
    this.maxSparseDataBytes = (int) (m * MAXIMUM_SPARSE_DATA_FRACTION);
    assert this.maxSparseDataBytes > 0;
    this.maxBufferElements = (int) (m * MAXIMUM_BUFFER_ELEMENTS_FRACTION);
    assert this.maxBufferElements > 0;

    // The default IntOpenHashSet rehashes all values. Since the values that we are adding are
    // already uniform hashes, we get rid of this overhead by using a custom identity function.
    this.buffer = new IntOpenCustomHashSet(IDENTITY_HASH_STRATEGY);

    // We have no good way of checking whether the data actually contains the given number of
    // elements without decoding the data, which would be inefficient here.
  }

  @Override
  public Representation addHash(long hash) {
    buffer.add(encoding.encode(hash));
    return updateRepresentation();
  }

  @Override
  public Representation addSparseValue(Encoding.Sparse encoding, int sparseValue) {
    this.encoding.assertCompatible(encoding);

    if (encoding.isLessThan(this.encoding)) {
      Representation repr = downgrade(encoding);
      return repr.addSparseValue(encoding, sparseValue);
    }

    if (this.encoding.isLessThan(encoding)) {
      buffer.add(encoding.downgrade(sparseValue, this.encoding));
    } else {
      buffer.add(sparseValue);
    }

    return updateRepresentation();
  }

  @Override
  public Representation addSparseValues(
      Encoding.Sparse encoding, @Nullable IntIterator sparseValues) {
    this.encoding.assertCompatible(encoding);

    // Downgrade ourselves if the incoming values are of lower precision.
    if (encoding.isLessThan(this.encoding)) {
      Representation repr = downgrade(encoding);
      return repr.addSparseValues(encoding, sparseValues);
    }

    // Nothing to merge. We do this late since we still want to downgrade our representation even
    // if there are no actual values to be merged.
    if (sparseValues == null) {
      return this;
    }

    // Downgrading the incoming values destroys their sort order so we need to add each value
    // individually to the buffer, compacting as necessary. This is much more expensive than merging
    // two sparse values of equal precision, unfortunately.
    if (this.encoding.isLessThan(encoding)) {
      IntIterator iter = encoding.downgrade(sparseValues, this.encoding);
      return addUnsortedSparseValues(this, this.encoding, iter);
    }

    // Special case when encodings are the same. Then we can profit from the fact that sparseValues
    // are sorted (as defined in the addSparseValues contract) and do a merge-join.
    IntIterator iter = sortedIterator();
    if (iter != null) {
      iter = new MergedIntIterator(iter, sparseValues);
    } else {
      iter = sparseValues;
    }

    // TODO: Merge without risking to grow this representation above its maximum size.
    set(this.encoding.dedupe(iter));
    return updateRepresentation();
  }

  private Representation downgrade(Encoding.Sparse encoding) {
    if (!encoding.isLessThan(this.encoding)) {
      return this;
    }

    GrowingByteSlice originalData = state.sparseData;
    state.sparseData = null;
    state.precision = Math.min(this.encoding.normalPrecision, encoding.normalPrecision);
    state.sparsePrecision = Math.min(this.encoding.sparsePrecision, encoding.sparsePrecision);
    Representation repr = new SparseRepresentation(state);

    // Add values from the backing data and from the buffer.
    if (originalData != null && originalData.hasRemaining()) {
      IntIterator iter = this.encoding.downgrade(new DifferenceDecoder(originalData), encoding);
      repr = addUnsortedSparseValues(repr, encoding, iter);
    }
    repr = addUnsortedSparseValues(repr, encoding, this.bufferIterator());

    return repr;
  }

  /** Individually adds unsorted values to a representation. */
  private static Representation addUnsortedSparseValues(
      Representation repr, Encoding.Sparse encoding, @Nullable IntIterator iter) {
    if (iter == null) {
      return repr;
    }

    while (iter.hasNext()) {
      repr = repr.addSparseValue(encoding, iter.next());
    }

    return repr;
  }

  @Override
  public Representation compact() {
    flushBuffer();

    // This is silly, but the Go and C++ implementations always serialize an empty byte array so we
    // set one here to be compatible in golden tests.
    if (this.state.sparseData == null) {
      this.state.sparseData = GrowingByteSlice.allocate(0);
    }

    return updateRepresentation();
  }

  @Override
  public long estimate() {
    flushBuffer();

    // Linear counting over the number of empty sparse buckets.
    int buckets = 1 << state.sparsePrecision;
    int numZeros = buckets - state.sparseSize;
    double estimate = buckets * Math.log((double) buckets / (double) numZeros);

    return Math.round(estimate);
  }

  @Override
  protected NormalRepresentation merge(NormalRepresentation other)
      throws IncompatiblePrecisionException {
    NormalRepresentation normal = normalize();
    return normal.merge(other);
  }

  @Override
  protected Representation merge(SparseRepresentation other) {
    // TODO: Add special case when 'this' is empty and 'other' has only encoded data.
    // In that case, we can just copy over the sparse data without needing to decode and dedupe.
    return this.addSparseValues(other.encoding, other.sortedIterator());
  }

  /**
   * Performance optimization that merges this representation into the given normal representation
   * without the overhead of merging and deduplicating elements that would be provided by the
   * {@link #dedupedIterator()} or {@link #sortedIterator()}.
   */
  void mergeInto(NormalRepresentation other) {
    if (state.hasSparseData()) {
      other.addSparseValues(encoding, new DifferenceDecoder(state.sparseData));
    }
    other.addSparseValues(encoding, buffer.iterator());
  }

  /**
   * Updates the sparse representation:
   *
   * <ol>
   *   <li>If the temporary list has become too large, serialize it into the sparse bytes
   *       representation.
   *   <li>If the sparse representation has become too large, converts to a {@link
   *       NormalRepresentation}.
   * </ol>
   *
   * @return a new normal representation if this sparse representation has outgrown itself or {@code
   *     this} if the sparse representation can continue to be be used
   */
  private Representation updateRepresentation() {
    // Flush the buffer if it is exceeding the maximum allowed amount of memory.
    if (buffer.size() > maxBufferElements) {
      flushBuffer();
    }

    // Upgrade to normal if the sparse data exceeds the maximum allowed amount of memory.
    //
    // Note that sparseData will allocate a larger buffer on the heap (of size
    // sparseData.capacity()) than is actually occupied by the sparse encoding (of size
    // sparseData.remaining()), since we cannot efficiently anticipate how many bytes will be
    // written when flushing the buffer. So in principle, we would need to compare
    // sparseData.capacity() with maxSparseDataBytes here if we wanted to make sure that we never
    // use too much memory at runtime. This would not be compatible with golden tests, though, which
    // ensure that the representation upgrades to normal just before the *serialized* sparse format
    // uses more memory than maxSparseDataBytes. I.e., we would be upgrading to normal
    // representation earlier than the golden tests.
    if (state.sparseData != null && state.sparseData.remaining() >= maxSparseDataBytes) {
      return normalize();
    }

    return this;
  }

  /** Convert to {@link NormalRepresentation}. */
  private NormalRepresentation normalize() {
    try {
      NormalRepresentation representation = new NormalRepresentation(state);
      representation = representation.merge(this);
      state.sparseData = null;
      state.sparseSize = 0;
      return representation;
    } catch (IncompatiblePrecisionException e) {
      throw new AssertionError("programming error", e);
    }
  }

  /**
   * Flushes the temporary {@link #buffer} of values into the difference encoded sparse {@link
   * State#sparseData}.
   */
  private void flushBuffer() {
    if (!buffer.isEmpty()) {
      set(dedupedIterator());
    }
  }

  /**
   * Returns an iterator over the sorted, de-duplicated values of the difference encoded data and
   * the temporary buffer, or {@code null} if both are empty.
   */
  @Nullable
  private IntIterator dedupedIterator() {
    IntIterator a = dataIterator();
    IntIterator b = bufferIterator();

    if (a != null && b != null) {
      return encoding.dedupe(new MergedIntIterator(a, b));
    }

    if (b != null) {
      return encoding.dedupe(b);
    }

    return a;
  }

  /**
   * Returns an iterator over the sorted (but not de-duplicated) values of the difference encoded
   * data and the temporary buffer, or {@code null} if both are empty.
   */
  @Nullable
  private IntIterator sortedIterator() {
    IntIterator a = dataIterator();
    IntIterator b = bufferIterator();

    if (a != null && b != null) {
      return new MergedIntIterator(a, b);
    }

    if (b != null) {
      return b;
    }

    return a;
  }

  /** Returns an iterator over the difference encoded data, or {@code null} if it is empty. */
  @Nullable
  private IntIterator dataIterator() {
    if (state.hasSparseData()) {
      return new DifferenceDecoder(state.sparseData);
    }
    return null;
  }

  /**
   * Returns an iterator over the values in the temporary buffer in sorted order, or {@code null} if
   * the temporary buffer is empty.
   */
  @Nullable
  private IntIterator bufferIterator() {
    // Create a sorted, deduped iterator over the values in the temporary buffer, merging the
    // values with the existing difference encoded data, if applicable.
    if (!buffer.isEmpty()) {
      int[] intArray = buffer.toIntArray();
      Arrays.sort(intArray);
      return IntIterators.wrap(intArray);
    }
    return null;
  }

  /** Sets the representation's values from {@code iter}. */
  private void set(@Nullable IntIterator iter) {
    GrowingByteSlice slice = getRecycledData();
    DifferenceEncoder encoder = new DifferenceEncoder(slice);

    int size = 0;
    while (iter != null && iter.hasNext()) {
      encoder.putInt(iter.nextInt());
      size++;
    }

    buffer.clear();
    recycledData = new WeakReference<>(state.sparseData);
    state.sparseData = slice.flip();
    state.sparseSize = size;
  }

  /** Returns the {@link #recycledData} if it is available, or a new ByteBuffer otherwise. */
  private GrowingByteSlice getRecycledData() {
    GrowingByteSlice slice = this.recycledData.get();
    if (slice != null) {
      slice.clear();
      return slice;
    }

    if (state.hasSparseData()) {
      // Assume that we'll write at least as many bytes as are currently set.
      return GrowingByteSlice.allocate(state.sparseData.remaining());
    }
    return GrowingByteSlice.allocate(0);
  }

  /** A hash strategy that uses the identity function as hash function. */
  private static final Strategy IDENTITY_HASH_STRATEGY =
      new Strategy() {
        @Override
        public int hashCode(int e) {
          return e;
        }

        @Override
        public boolean equals(int a, int b) {
          return a == b;
        }
      };

  private static void checkPrecision(int normalPrecision, int sparsePrecision) {
    NormalRepresentation.checkPrecision(normalPrecision);
    Preconditions.checkArgument(
        normalPrecision <= sparsePrecision && sparsePrecision <= MAXIMUM_SPARSE_PRECISION,
        "Expected sparse precision to be >= normal precision (%s) and <= "
            + MAXIMUM_SPARSE_PRECISION
            + " but was %s.",
        normalPrecision,
        sparsePrecision);
  }
}
