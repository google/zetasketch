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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.WireFormat;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.AggregatorType;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.HllplusUnique;
import com.google.protos.zetasketch.HllplusUnique.HyperLogLogPlusUniqueStateProto;
import com.google.zetasketch.ValueType;
import com.google.zetasketch.internal.ByteSlice;
import com.google.zetasketch.internal.GrowingByteSlice;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;

/**
 * Representation of a HyperLogLog++ state. In contrast to using just a normal protocol buffer
 * representation, this object has the advantage of providing a simpler interface for the values
 * relevant to HyperLogLog++ as well as fast and low-memory (aliased) parsing.
 */
public class State {

  /**
   * The type of the aggregator.
   *
   * @see AggregatorStateProto#getType()
   */
  public AggregatorType type;

  /**
   * The number of values that the aggregator has seen.
   *
   * @see AggregatorStateProto#getNumValues()
   */
  public long numValues;

  /**
   * Version of the encoded internal state.
   *
   * @see AggregatorStateProto#getEncodingVersion()
   */
  public int encodingVersion;

  /**
   * The value type for the aggregation
   *
   * @see AggregatorStateProto#getValueType()
   */
  public ValueType valueType;

  /**
   * Size of sparse list, i.e., how many different indexes are present in {@link #sparseData}.
   *
   * @see HyperLogLogPlusUniqueStateProto#getSparseSize()
   */
  public int sparseSize;

  /**
   * Precision / number of buckets for the normal representation.
   *
   * @see HyperLogLogPlusUniqueStateProto#getPrecisionOrNumBuckets()
   */
  public int precision;

  /**
   * Precision / number of buckets for the sparse representation.
   *
   * @see HyperLogLogPlusUniqueStateProto#getSparsePrecisionOrNumBuckets()
   */
  public int sparsePrecision;

  /**
   * Normal data representation.
   *
   * @see HyperLogLogPlusUniqueStateProto#getData()
   */
  @Nullable public ByteSlice data;

  /**
   * Sparse data representation.
   *
   * @see HyperLogLogPlusUniqueStateProto#getSparseData()
   */
  @Nullable public GrowingByteSlice sparseData;

  /** Constructs a new state with default values. */
  public State() {
    clear();
  }

  /** Resets all fields to their default value. */
  // Using UnknownInitialization as this method is called both from the constructor (when it would
  // be UnderInitialization(State.class)) as well as by users manually when wishing to reset the
  // state (when it is Initialized(State.class)).
  // https://checkerframework.org/manual/#initialization-checker
  @EnsuresNonNull({"type", "valueType"})
  public void clear(@UnknownInitialization State this) {
    type = DEFAULT_TYPE;
    numValues = DEFAULT_NUM_VALUES;
    encodingVersion = DEFAULT_ENCODING_VERSION;
    valueType = DEFAULT_VALUE_TYPE;
    sparseSize = DEFAULT_SPARSE_SIZE;
    precision = DEFAULT_PRECISION_OR_NUM_BUCKETS;
    sparsePrecision = DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS;
    data = null;
    sparseData = null;
  }

  /** Returns whether the state has at least one byte of readable {@link #data}. */
  @EnsuresNonNullIf(expression = "this.data", result = true)
  public boolean hasData() {
    return data != null && data.hasRemaining();
  }

  /** Returns whether the state has at least one byte of readable {@link #sparseData}. */
  @EnsuresNonNullIf(expression = "this.sparseData", result = true)
  public boolean hasSparseData() {
    return sparseData != null && sparseData.hasRemaining();
  }

  // Protocol buffer tags consist of the field number concatenated with the field type. Because we
  // use these in case statements below, they must be constant expressions and the bitshift can not
  // be refactored into a method.
  private static final int TYPE_TAG =
      AggregatorStateProto.TYPE_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int NUM_VALUES_TAG =
      AggregatorStateProto.NUM_VALUES_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int ENCODING_VERSION_TAG =
      AggregatorStateProto.ENCODING_VERSION_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int VALUE_TYPE_TAG =
      AggregatorStateProto.VALUE_TYPE_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int HYPERLOGLOGPLUS_UNIQUE_STATE_TAG =
      HllplusUnique.HYPERLOGLOGPLUS_UNIQUE_STATE_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static final AggregatorType DEFAULT_TYPE = AggregatorType.HYPERLOGLOG_PLUS_UNIQUE;
  private static final int DEFAULT_NUM_VALUES = 0;
  private static final int DEFAULT_ENCODING_VERSION = 1;
  private static final ValueType DEFAULT_VALUE_TYPE = ValueType.UNKNOWN;

  /**
   * Parses a serialized HyperLogLog++ {@link AggregatorStateProto} and populates this object's
   * fields. Note that {@link #data} and {@link #sparseData} will <em>alias</em> the given bytes
   * &mdash; sharing the same memory.
   *
   * @throws IOException If the stream does not contain a serialized {@link AggregatorStateProto} or
   *     if fields are set that would typically not belong
   */
  public void parse(byte[] bytes) throws IOException {
    CodedInputStream stream = CodedInputStream.newInstance(bytes);
    stream.enableAliasing(true);
    parse(stream);
  }

  /**
   * Parses a serialized HyperLogLog++ {@link AggregatorStateProto} and populates this object's
   * fields. If the {@code input} supports aliasing (for byte arrays and {@link ByteBuffer}, see
   * {@link CodedInputStream#enableAliasing(boolean) for details}), {@link #data} and {@link
   * #sparseData} will <em>alias</em> the given bytes &mdash; sharing the same memory.
   *
   * @throws IOException If the stream does not contain a serialized {@link AggregatorStateProto} or
   *     if fields are set that would typically not belong
   */
  public void parse(CodedInputStream input) throws IOException {
    // Reset defaults as values set to the default will not be encoded in the protocol buffer.
    clear();

    UnknownFieldSet.Builder ignoredFields = UnknownFieldSet.newBuilder();
    while (!input.isAtEnd()) {
      int tag = input.readTag();
      switch (tag) {
        case TYPE_TAG:
          type = AggregatorType.forNumber(input.readEnum());
          break;
        case NUM_VALUES_TAG:
          numValues = input.readInt64();
          break;
        case ENCODING_VERSION_TAG:
          encodingVersion = input.readInt32();
          break;
        case VALUE_TYPE_TAG:
          valueType = ValueType.forNumber(input.readEnum());
          break;
        case HYPERLOGLOGPLUS_UNIQUE_STATE_TAG:
          parseHll(input, input.readInt32());
          break;
        default:
          ignoredFields.mergeFieldFrom(tag, input);
      }
    }
  }

  public byte[] toByteArray() {
    try {
      final byte[] result = new byte[getSerializedSize()];
      final CodedOutputStream output = CodedOutputStream.newInstance(result);
      writeTo(output);
      output.checkNoSpaceLeft();
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected IOException serializing to byte array", e);
    }
  }

  public void writeTo(CodedOutputStream stream) throws IOException {
    // We use the NoTag write methods for consistency with the parsing functions and for
    // consistency with the variable-length writes where we can't use any convenience function.
    stream.writeUInt32NoTag(TYPE_TAG);
    stream.writeEnumNoTag(type.getNumber());

    stream.writeUInt32NoTag(NUM_VALUES_TAG);
    stream.writeInt64NoTag(numValues);

    if (encodingVersion != DEFAULT_ENCODING_VERSION) {
      stream.writeUInt32NoTag(ENCODING_VERSION_TAG);
      stream.writeInt32NoTag(encodingVersion);
    }

    if (!valueType.equals(DEFAULT_VALUE_TYPE)) {
      stream.writeUInt32NoTag(VALUE_TYPE_TAG);
      stream.writeEnumNoTag(valueType.getNumber());
    }

    stream.writeUInt32NoTag(HYPERLOGLOGPLUS_UNIQUE_STATE_TAG);
    stream.writeUInt32NoTag(getSerializedHllSize());
    writeHllTo(stream);
  }

  public int getSerializedSize() {
    int size = 0;

    size += CodedOutputStream.computeUInt32SizeNoTag(TYPE_TAG);
    size += CodedOutputStream.computeEnumSizeNoTag(type.getNumber());

    size += CodedOutputStream.computeUInt32SizeNoTag(NUM_VALUES_TAG);
    size += CodedOutputStream.computeInt64SizeNoTag(numValues);

    if (encodingVersion != DEFAULT_ENCODING_VERSION) {
      size += CodedOutputStream.computeUInt32SizeNoTag(ENCODING_VERSION_TAG);
      size += CodedOutputStream.computeInt32SizeNoTag(encodingVersion);
    }

    if (!valueType.equals(DEFAULT_VALUE_TYPE)) {
      size += CodedOutputStream.computeUInt32SizeNoTag(VALUE_TYPE_TAG);
      size += CodedOutputStream.computeEnumSizeNoTag(valueType.getNumber());
    }

    int hllSize = getSerializedHllSize();
    size += CodedOutputStream.computeUInt32SizeNoTag(HYPERLOGLOGPLUS_UNIQUE_STATE_TAG);
    size += CodedOutputStream.computeUInt32SizeNoTag(hllSize);
    size += hllSize;

    return size;
  }

  private static final int SPARSE_SIZE_TAG =
      HyperLogLogPlusUniqueStateProto.SPARSE_SIZE_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int PRECISION_OR_NUM_BUCKETS_TAG =
      HyperLogLogPlusUniqueStateProto.PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int SPARSE_PRECISION_OR_NUM_BUCKETS_TAG =
      HyperLogLogPlusUniqueStateProto.SPARSE_PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_VARINT;
  private static final int DATA_TAG =
      HyperLogLogPlusUniqueStateProto.DATA_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int SPARSE_DATA_TAG =
      HyperLogLogPlusUniqueStateProto.SPARSE_DATA_FIELD_NUMBER << 3
          | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static final int DEFAULT_SPARSE_SIZE = 0;
  private static final int DEFAULT_PRECISION_OR_NUM_BUCKETS = 0;
  private static final int DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS = 0;

  /**
   * Parses a {@link HyperLogLogPlusUniqueStateProto} message. Since the message is nested within an
   * {@link AggregatorStateProto}, we limit ourselves to reading only the bytes of the specified
   * message length.
   */
  private void parseHll(CodedInputStream input, int size) throws IOException {
    int limit = input.getTotalBytesRead() + size;

    ByteBuffer buffer;
    UnknownFieldSet.Builder ignoredFields = UnknownFieldSet.newBuilder();
    while (input.getTotalBytesRead() < limit && !input.isAtEnd()) {
      int tag = input.readTag();
      switch (tag) {
        case SPARSE_SIZE_TAG:
          sparseSize = input.readInt32();
          break;
        case PRECISION_OR_NUM_BUCKETS_TAG:
          precision = input.readInt32();
          break;
        case SPARSE_PRECISION_OR_NUM_BUCKETS_TAG:
          sparsePrecision = input.readInt32();
          break;
        case DATA_TAG:
          buffer = input.readByteBuffer();
          data = ByteSlice.copyOnWrite(buffer);
          break;
        case SPARSE_DATA_TAG:
          buffer = input.readByteBuffer();
          sparseData = GrowingByteSlice.copyOnWrite(buffer);
          break;
        default:
          ignoredFields.mergeFieldFrom(tag, input);
      }
    }
  }

  private void writeHllTo(CodedOutputStream stream) throws IOException {
    // We use the NoTag write methods for consistency with the parsing functions and for
    // consistency with the variable-length writes where we can't use any convenience function.
    if (sparseSize != DEFAULT_SPARSE_SIZE) {
      stream.writeUInt32NoTag(SPARSE_SIZE_TAG);
      stream.writeInt32NoTag(sparseSize);
    }

    if (precision != DEFAULT_PRECISION_OR_NUM_BUCKETS) {
      stream.writeUInt32NoTag(PRECISION_OR_NUM_BUCKETS_TAG);
      stream.writeInt32NoTag(precision);
    }

    if (sparsePrecision != DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS) {
      stream.writeUInt32NoTag(SPARSE_PRECISION_OR_NUM_BUCKETS_TAG);
      stream.writeInt32NoTag(sparsePrecision);
    }

    // Static analysis can not verify that stream.writeUInt32NoTag does not null out this.data
    final ByteSlice data = this.data;
    if (data != null) {
      stream.writeUInt32NoTag(DATA_TAG);
      stream.writeUInt32NoTag(data.remaining());
      stream.writeLazy(data.byteBuffer());
    }

    // Static analysis can not verify that stream.writeUInt32NoTag does not null out this.sparseData
    final GrowingByteSlice sparseData = this.sparseData;
    if (sparseData != null) {
      stream.writeUInt32NoTag(SPARSE_DATA_TAG);
      stream.writeUInt32NoTag(sparseData.remaining());
      stream.writeLazy(sparseData.byteBuffer());
    }
  }

  private int getSerializedHllSize() {
    int size = 0;

    if (sparseSize != DEFAULT_SPARSE_SIZE) {
      size += CodedOutputStream.computeUInt32SizeNoTag(SPARSE_SIZE_TAG);
      size += CodedOutputStream.computeInt32SizeNoTag(sparseSize);
    }

    if (precision != DEFAULT_PRECISION_OR_NUM_BUCKETS) {
      size += CodedOutputStream.computeUInt32SizeNoTag(PRECISION_OR_NUM_BUCKETS_TAG);
      size += CodedOutputStream.computeInt32SizeNoTag(precision);
    }

    if (sparsePrecision != DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS) {
      size += CodedOutputStream.computeUInt32SizeNoTag(SPARSE_PRECISION_OR_NUM_BUCKETS_TAG);
      size += CodedOutputStream.computeInt32SizeNoTag(sparsePrecision);
    }

    if (data != null) {
      int dataLength = data.remaining();
      size += CodedOutputStream.computeUInt32SizeNoTag(DATA_TAG);
      size += CodedOutputStream.computeUInt32SizeNoTag(dataLength);
      size += dataLength;
    }

    if (sparseData != null) {
      int sparseDataLength = sparseData.remaining();
      size += CodedOutputStream.computeUInt32SizeNoTag(SPARSE_DATA_TAG);
      size += CodedOutputStream.computeUInt32SizeNoTag(sparseDataLength);
      size += sparseDataLength;
    }

    return size;
  }
}
