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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.AggregatorType;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.HllplusUnique;
import com.google.zetasketch.internal.hash.Hash;
import com.google.zetasketch.internal.hllplus.NormalRepresentation;
import com.google.zetasketch.internal.hllplus.Representation;
import com.google.zetasketch.internal.hllplus.SparseRepresentation;
import com.google.zetasketch.internal.hllplus.State;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

/**
 * HLL++ aggregator for estimating cardinalities of multisets.
 *
 * <p>The aggregator uses the standard format for storing the internal state of the cardinality
 * estimate as defined in hllplus-unique.proto, allowing users to merge aggregators with data
 * computed in C++ or Go and to load up the cardinalities in a variety of analysis tools.
 *
 * <p>The precision defines the accuracy of the HLL++ aggregator at the cost of the memory used. The
 * upper bound on the memory required is 2<sup>precision</sup> bytes, but less memory is used for
 * smaller cardinalities (up to ~2<sup>precision - 2</sup>). The relative error is 1.04 /
 * sqrt(2<sup>precision</sup>). A typical value used at Google is 15, which gives an error of about
 * 0.6% while requiring an upper bound of 32&nbsp;KiB of memory.
 *
 * <p>Note that this aggregator is <em>not</em> designed to be thread safe.
 */
public final class HyperLogLogPlusPlus<V> implements Aggregator<V, Long, HyperLogLogPlusPlus<V>> {

  /** The smallest normal precision supported by this aggregator. */
  public static final int MINIMUM_PRECISION = NormalRepresentation.MINIMUM_PRECISION;

  /** The largest normal precision supported by this aggregator. */
  public static final int MAXIMUM_PRECISION = NormalRepresentation.MAXIMUM_PRECISION;

  /** The default normal precision that is used if the user does not specify a normal precision. */
  public static final int DEFAULT_NORMAL_PRECISION = 15;

  /** The largest sparse precision supported by this aggregator. */
  public static final int MAXIMUM_SPARSE_PRECISION = SparseRepresentation.MAXIMUM_SPARSE_PRECISION;

  /** Value used to indicate that the sparse representation should not be used. */
  public static final int SPARSE_PRECISION_DISABLED = Representation.SPARSE_PRECISION_DISABLED;

  /**
   * If no sparse precision is specified, this value is added to the normal precision to obtain the
   * sparse precision, which optimizes the memory-precision trade-off.
   *
   */
  public static final int DEFAULT_SPARSE_PRECISION_DELTA = 5;

  /** The encoding version of the {@link AggregatorStateProto}. We only support v2. */
  private static final int ENCODING_VERSION = 2;

  private static final ExtensionRegistry EXTENSIONS;

  static {
    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    HllplusUnique.registerAllExtensions(registry);
    EXTENSIONS = registry.getUnmodifiable();
  }

  /**
   * Creates a new HyperLogLog++ aggregator from {@code proto}. Note that this method is quite
   * inefficient as it will re-serialize the proto in order to parse it into an internal structure.
   * It is preferable to use {@link #forProto(byte[])}
   * whenever possible.
   *
   * @param proto a valid aggregator state of type {@link AggregatorType#HYPERLOGLOG_PLUS_UNIQUE}.
   */
  public static HyperLogLogPlusPlus<?> forProto(AggregatorStateProto proto) {
    return forProto(proto.toByteArray());
  }

  /**
   * Creates a new HyperLogLog++ aggregator from the serialized {@code proto}.
   *
   * @param proto a valid aggregator state of type {@link AggregatorType#HYPERLOGLOG_PLUS_UNIQUE}.
   */
  public static HyperLogLogPlusPlus<?> forProto(ByteString proto) {
    return forProto(proto.newCodedInput());
  }

  /**
   * Creates a new HyperLogLog++ aggregator from the serialized {@code proto}.
   *
   * <p><strong>Important:</strong> while the aggregator will never modify the byte array it may
   * retain a reference to its data for a long time if no write operations are called. Clients
   * wishing to make changes to the buffer should make a defensive copy of the data before passing
   * it to this method.
   *
   * @param proto a valid aggregator state of type {@link AggregatorType#HYPERLOGLOG_PLUS_UNIQUE}.
   */
  public static HyperLogLogPlusPlus<?> forProto(byte[] proto) {
    return forProto(CodedInputStream.newInstance(proto));
  }

  private static HyperLogLogPlusPlus<?> forProto(CodedInputStream proto) {
    try {
      // Enable aliasing if possible to avoid unnecessary data copies. This is safe since the
      // ByteSlice used in the State employs copy-on-write.
      State state = new State();
      proto.enableAliasing(true);
      state.parse(proto);
      return new HyperLogLogPlusPlus<>(state);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * The types of values that can be added to this aggregator. Used for type safety in the {@link
   * #add(Object)} methods (including the {@code add} methods for primitive types) to ensure that
   * users don't accidentally add incompatible types.
   */
  private final Set<Type> allowedTypes;

  /** Backing state of the HyperLogLog++ representation. */
  private final State state;

  /**
   * The HLL++ representation. Can be one of {@link SparseRepresentation} or {@link
   * NormalRepresentation}.
   */
  private Representation representation;

  private HyperLogLogPlusPlus(State state) {
    Preconditions.checkArgument(
        state.type == AggregatorType.HYPERLOGLOG_PLUS_UNIQUE,
        "Expected proto to be of type HYPERLOGLOG_PLUS_UNIQUE but was %s",
        state.type);
    Preconditions.checkArgument(
        state.encodingVersion == ENCODING_VERSION,
        "Expected encoding version to be " + ENCODING_VERSION + " but was %s",
        state.encodingVersion);

    this.state = state;
    allowedTypes = Type.extractAndNormalize(state);
    representation = Representation.fromState(state);
  }

  /**
   * Adds {@code value} to the aggregator. This provides a performance optimization over {@link
   * #add(Object)} to avoid unnecessary boxing.
   *
   * @see #add(Object)
   */
  public void add(int value) throws IllegalArgumentException {
    checkAndSetType(Type.INTEGER);
    addHash(Hash.of(value));
  }

  /**
   * Adds {@code value} to the aggregator. This provides a performance optimization over {@link
   * #add(Object)} to avoid unnecessary boxing.
   *
   * @see #add(Object)
   */
  public void add(long value) throws IllegalArgumentException {
    checkAndSetType(Type.LONG);
    addHash(Hash.of(value));
  }

  /**
   * Adds {@code value} to the aggregator. This provides a performance optimization over {@link
   * #add(Object)} to avoid unnecessary boxing.
   *
   * @see #add(Object)
   */
  public void add(byte[] value) throws IllegalArgumentException {
    checkAndSetType(Type.BYTES);
    addHash(Hash.of(value));
  }

  @Override
  public void add(V value) throws IllegalArgumentException {
    if (value instanceof String) {
      checkAndSetType(Type.STRING);
      addHash(Hash.of((String) value));
    } else if (value instanceof ByteString) {
      add(((ByteString) value).toByteArray());
    } else if (value instanceof Integer) {
      add(((Integer) value).intValue());
    } else if (value instanceof Long) {
      add(((Long) value).longValue());
    } else {
      throw new IllegalArgumentException(
          "unable to add " + value.getClass() + " to aggregator of type " + allowedTypes);
    }
  }

  private void addHash(long hash) {
    representation = representation.addHash(hash);
    state.numValues++;
  }

  /**
   * Returns the cardinality estimate of this aggregator.
   *
   * <p>This is like {@link #result()} except that we avoid the boxing into a {@link Long}.
   */
  public long longResult() {
    return representation.estimate();
  }

  @Override
  public void merge(HyperLogLogPlusPlus<V> aggregator) throws IncompatiblePrecisionException {
    if (aggregator != null) {
      checkTypeAndMerge(aggregator);
    }
  }

  @Override
  public void merge(AggregatorStateProto proto) throws IllegalArgumentException {
    if (proto != null) {
      checkTypeAndMerge(HyperLogLogPlusPlus.forProto(proto));
    }
  }

  public void merge(byte[] proto) {
    if (proto != null && proto.length > 0) {
      checkTypeAndMerge(HyperLogLogPlusPlus.forProto(proto));
    }
  }

  @Override
  public void merge(ByteString proto) {
    if (proto != null && !proto.isEmpty()) {
      checkTypeAndMerge(HyperLogLogPlusPlus.forProto(proto));
    }
  }

  private void checkTypeAndMerge(HyperLogLogPlusPlus<?> other) {
    Set<Type> newTypes = EnumSet.copyOf(allowedTypes);
    newTypes.retainAll(other.allowedTypes);
    Preconditions.checkArgument(
        !newTypes.isEmpty(),
        "Aggregator of type %s is incompatible with aggregator of type %s",
        allowedTypes,
        other.allowedTypes);
    allowedTypes.clear();
    allowedTypes.addAll(newTypes);

    representation = representation.merge(other.representation);
    state.numValues += other.state.numValues;
  }

  @Override
  public long numValues() {
    return state.numValues;
  }

  @Override
  public Long result() {
    return Long.valueOf(longResult());
  }

  @Override
  public AggregatorStateProto serializeToProto() {
    try {
      return AggregatorStateProto.parseFrom(serializeToByteArray(), EXTENSIONS);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Invalid serialized state", e);
    }
  }

  /**
   * Returns the internal state of this aggregator as a serialized byte array.
   *
   * <p>This is currently faster than calling the semantically equivalent {@code
   * aggregator.serializeToProto().toByteArray()} due to custom proto serialization.
   */
  public byte[] serializeToByteArray() {
    representation = representation.compact();
    return state.toByteArray();
  }

  @Override
  public ByteString serializeToByteString() {
    // TODO: Remove serializeToByteString method or get rid of this copying.
    return ByteString.copyFrom(serializeToByteArray());
  }

  public int getNormalPrecision() {
    return state.precision;
  }

  public int getSparsePrecision() {
    return state.sparsePrecision;
  }

  /**
   * Checks if the type is supported by this aggregator, raising an {@link IllegalArgumentException}
   * if not. If allowed, then also narrows the supported types to only allow the given type for
   * future calls.
   */
  private void checkAndSetType(Type type) throws IllegalArgumentException {
    Preconditions.checkArgument(
        allowedTypes.contains(type),
        "unable to add type %s to aggregator of type %s",
        type,
        allowedTypes);

    // Narrow the type if necessary.
    if (allowedTypes.size() > 1) {
      allowedTypes.clear();
      allowedTypes.add(type);
      state.valueType = type.valueType;
    }
  }

  /** Enumeration of types permitted by the aggregator. */
  private enum Type {
    LONG(ValueType.forStandardType(DefaultOpsType.Id.UINT64)),
    INTEGER(ValueType.forStandardType(DefaultOpsType.Id.UINT32)),
    STRING(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING)),
    BYTES(ValueType.forStandardType(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));

    /** Returns the permissible types for the given state. */
    public static Set<Type> extractAndNormalize(State state) {
      if (state.valueType.equals(ValueType.UNKNOWN)) {
        return EnumSet.allOf(Type.class);
      }

      switch (state.valueType.asDefaultOpsType()) {
        case UINT64:
          return EnumSet.of(LONG);
        case UINT32:
          return EnumSet.of(INTEGER);
        case BYTES_OR_UTF8_STRING:
          return EnumSet.of(STRING, BYTES);
        default:
          // Not supported - fall through to throw.
      }
      throw new IllegalArgumentException("Unsupported value type " + state.valueType);
    }

    /** The {@link ValueType} corresponding to this aggregator type. */
    private final ValueType valueType;

    Type(ValueType valueType) {
      this.valueType = valueType;
    }
  }

  /**
   * Builder for new {@link HyperLogLogPlusPlus} aggregators.
   *
   * <p>Optional parameters are set with {@link #normalPrecision(int)} and {@link
   * #sparsePrecision(int)}. The mandatory type parameter is set by invoking any of the {@code
   * buildFor<Type>} methods.
   *
   * <p>Builders with the precision set can be reused to build multiple aggregators.
   */
  public static class Builder {

    private int normalPrecision = DEFAULT_NORMAL_PRECISION;
    private int sparsePrecision;

    private boolean sparsePrecisionExplicitlySet = false;

    /**
     * Sets the normal precision to be used. Must be in the range from {@link #MINIMUM_PRECISION} to
     * {@link #MAXIMUM_PRECISION} (inclusive).
     *
     * <p>The precision defines the accuracy of the HLL++ aggregator at the cost of the memory used.
     * The upper bound on the memory required is 2<sup>precision</sup> bytes, but less memory is
     * used for smaller cardinalities (up to ~2<sup>precision - 2</sup>). The relative error is 1.04
     * / sqrt(2<sup>precision</sup>). If not specified, {@link #DEFAULT_NORMAL_PRECISION} is used,
     * which gives an error of about 0.6% while requiring an upper bound of 32&nbsp;KiB of memory.
     */
    public Builder normalPrecision(int normalPrecision) {
      // TODO: Validate precision here by calling NormalRepresentation#checkPrecision(..)?

      this.normalPrecision = normalPrecision;
      return this;
    }

    /**
     * Sets the sparse precision to be used. Must be in the range from the {@code normalPrecision}
     * to {@link #MAXIMUM_SPARSE_PRECISION} (inclusive), or {@link #SPARSE_PRECISION_DISABLED} to
     * disable the use of the sparse representation. We recommend to use {@link #noSparseMode()} for
     * the latter, though.
     *
     * <p>If not specified, the normal precision + {@link #DEFAULT_SPARSE_PRECISION_DELTA} is used.
     */
    public Builder sparsePrecision(int sparsePrecision) {
      this.sparsePrecision = sparsePrecision;
      sparsePrecisionExplicitlySet = true;
      return this;
    }

    /**
     * Disables the "sparse representation" mode; i.e., the normal representation, where all
     * registers are explicitly stored, and its method to compute the COUNT DISTINCT estimate are
     * used from the start of the aggregation.
     */
    public Builder noSparseMode() {
      return sparsePrecision(SPARSE_PRECISION_DISABLED);
    }

    private State buildState(DefaultOpsType.Id opsType) {
      State state = new State();
      state.type = AggregatorType.HYPERLOGLOG_PLUS_UNIQUE;
      state.encodingVersion = ENCODING_VERSION;
      state.precision = normalPrecision;

      // If sparse precision is not explicitly set, use default offset from normal precision.
      state.sparsePrecision =
          sparsePrecisionExplicitlySet
              ? sparsePrecision
              : normalPrecision + DEFAULT_SPARSE_PRECISION_DELTA;

      state.valueType = ValueType.forStandardType(opsType);
      return state;
    }

    /** Returns a new HLL++ aggregator for counting the number of unique byte arrays in a stream. */
    public HyperLogLogPlusPlus<ByteString> buildForBytes() {
      return new HyperLogLogPlusPlus<>(buildState(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));
    }

    /** Returns a new HLL++ aggregator for counting the number of unique integers in a stream. */
    public HyperLogLogPlusPlus<Integer> buildForIntegers() {
      return new HyperLogLogPlusPlus<>(buildState(DefaultOpsType.Id.UINT32));
    }

    /** Returns a new HLL++ aggregator for counting the number of unique longs in a stream. */
    public HyperLogLogPlusPlus<Long> buildForLongs() {
      return new HyperLogLogPlusPlus<>(buildState(DefaultOpsType.Id.UINT64));
    }

    /** Returns a new HLL++ aggregator for counting the number of unique strings in a stream. */
    public HyperLogLogPlusPlus<String> buildForStrings() {
      return new HyperLogLogPlusPlus<>(buildState(DefaultOpsType.Id.BYTES_OR_UTF8_STRING));
    }
  }
}
