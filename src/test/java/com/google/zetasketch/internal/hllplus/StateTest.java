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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Range;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protos.zetasketch.Aggregator.AggregatorStateProto;
import com.google.protos.zetasketch.Aggregator.AggregatorType;
import com.google.protos.zetasketch.Aggregator.DefaultOpsType;
import com.google.protos.zetasketch.CustomValueType;
import com.google.protos.zetasketch.HllplusUnique;
import com.google.protos.zetasketch.HllplusUnique.HyperLogLogPlusUniqueStateProto;
import com.google.zetasketch.ValueType;
import com.google.zetasketch.internal.ByteSlice;
import com.google.zetasketch.internal.GrowingByteSlice;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link State}. */
@RunWith(JUnit4.class)
public class StateTest {

  @Test
  public void hasData() {
    State state = new State();

    state.data = null;
    assertFalse(state.hasData());

    state.data = ByteSlice.allocate(0);
    assertFalse(state.hasData());

    state.data = ByteSlice.allocate(5).position(5);
    assertFalse(state.hasData());

    state.data = ByteSlice.allocate(5);
    assertTrue(state.hasData());
  }

  @Test
  public void hasSparseData() {
    State state = new State();

    state.sparseData = null;
    assertFalse(state.hasSparseData());

    state.sparseData = GrowingByteSlice.allocate(0);
    assertFalse(state.hasSparseData());

    state.sparseData = GrowingByteSlice.allocate(5).position(5);
    assertFalse(state.hasSparseData());

    state.sparseData = GrowingByteSlice.allocate(5);
    assertTrue(state.hasSparseData());
  }

  @Test
  public void parseType() throws IOException {
    AggregatorStateProto.Builder builder = AggregatorStateProto.newBuilder();

    // Required field should always be set.
    State state = parse(builder.clearType());
    assertEquals(
        "State should assume HLL++ by default as it is a required field",
        AggregatorType.HYPERLOGLOG_PLUS_UNIQUE, state.type);

    for (AggregatorType type : AggregatorType.values()) {
      state = parse(builder.setType(type));
      assertEquals(type, state.type);
    }
  }

  @Test
  public void parseNumValues() throws IOException {
    AggregatorStateProto.Builder builder = AggregatorStateProto.newBuilder();
    State state;

    state = parse(builder.clearNumValues());
    assertEquals(0, state.numValues);

    state = parse(builder.setNumValues(0));
    assertEquals(0, state.numValues);

    state = parse(builder.setNumValues(42));
    assertEquals(42, state.numValues);

    state = parse(builder.setNumValues(Long.MIN_VALUE));
    assertEquals(Long.MIN_VALUE, state.numValues);

    state = parse(builder.setNumValues(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, state.numValues);
  }

  @Test
  public void parseEncodingVersion() throws IOException {
    AggregatorStateProto.Builder builder = AggregatorStateProto.newBuilder();
    State state;

    state = parse(builder.clearEncodingVersion());
    assertEquals(
        "Expected the encoding version to be 1 since this is the default in the proto descriptor",
        1, state.encodingVersion);

    state = parse(builder.setEncodingVersion(0));
    assertEquals(0, state.encodingVersion);

    state = parse(builder.setEncodingVersion(42));
    assertEquals(42, state.encodingVersion);

    state = parse(builder.setEncodingVersion(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, state.encodingVersion);

    state = parse(builder.setEncodingVersion(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, state.encodingVersion);
  }

  @Test
  public void parseValueType_unknown() throws IOException {
    assertThat(parse(AggregatorStateProto.newBuilder()).valueType).isEqualTo(ValueType.UNKNOWN);
  }

  @Test
  public void parseValueType_DefaultOpsType() throws IOException {
    for (DefaultOpsType.Id opsType : DefaultOpsType.Id.values()) {
      ValueType valueType = ValueType.forStandardType(opsType);
      assertThat(parse(valueType.copyToBuilder(AggregatorStateProto.newBuilder())).valueType)
          .isEqualTo(valueType);
    }
  }

  @Test
  public void parseValueType_CustomValueType() throws IOException {
    for (CustomValueType.Id customType : CustomValueType.Id.values()) {
      AggregatorStateProto.Builder builder =
          ValueType.forCustomType(customType).copyToBuilder(AggregatorStateProto.newBuilder());
      assertThat(parse(builder).valueType).isEqualTo(ValueType.forStateProto(builder));
    }
  }

  @Test
  public void parseValueType_otherNumber() throws IOException {
    AggregatorStateProto.Builder builder =
        ValueType.forNumber(12345).copyToBuilder(AggregatorStateProto.newBuilder());
    assertThat(parse(builder).valueType).isEqualTo(ValueType.forStateProto(builder));
  }

  @Test
  public void parseSparseSize() throws IOException {
    HyperLogLogPlusUniqueStateProto.Builder builder = HyperLogLogPlusUniqueStateProto.newBuilder();
    State state;

    state = parse(builder.clearSparseSize());
    assertEquals(0, state.sparseSize);

    state = parse(builder.setSparseSize(0));
    assertEquals(0, state.sparseSize);

    state = parse(builder.setSparseSize(42));
    assertEquals(42, state.sparseSize);

    state = parse(builder.setSparseSize(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, state.sparseSize);

    state = parse(builder.setSparseSize(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, state.sparseSize);
  }

  @Test
  public void parsePrecision() throws IOException {
    HyperLogLogPlusUniqueStateProto.Builder builder = HyperLogLogPlusUniqueStateProto.newBuilder();
    State state;

    state = parse(builder.clearPrecisionOrNumBuckets());
    assertEquals(0, state.precision);

    state = parse(builder.setPrecisionOrNumBuckets(0));
    assertEquals(0, state.precision);

    state = parse(builder.setPrecisionOrNumBuckets(42));
    assertEquals(42, state.precision);

    state = parse(builder.setPrecisionOrNumBuckets(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, state.precision);

    state = parse(builder.setPrecisionOrNumBuckets(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, state.precision);
  }

  @Test
  public void parseSparsePrecision() throws IOException {
    HyperLogLogPlusUniqueStateProto.Builder builder = HyperLogLogPlusUniqueStateProto.newBuilder();
    State state;

    state = parse(builder.clearSparsePrecisionOrNumBuckets());
    assertEquals(0, state.sparsePrecision);

    state = parse(builder.setSparsePrecisionOrNumBuckets(0));
    assertEquals(0, state.sparsePrecision);

    state = parse(builder.setSparsePrecisionOrNumBuckets(42));
    assertEquals(42, state.sparsePrecision);

    state = parse(builder.setSparsePrecisionOrNumBuckets(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, state.sparsePrecision);

    state = parse(builder.setSparsePrecisionOrNumBuckets(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, state.sparsePrecision);
  }

  @Test
  public void parseData() throws IOException {
    HyperLogLogPlusUniqueStateProto.Builder builder = HyperLogLogPlusUniqueStateProto.newBuilder();
    State state;

    state = parse(builder.clearData());
    assertThat(state.data).isNull();

    state = parse(builder.setData(ByteString.EMPTY));
    assertThat(state.data.toByteArray()).isEmpty();

    state = parse(builder.setData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    assertThat(state.data.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
  }

  /** Verify that the data is aliased when possible. */
  @Test
  @SuppressWarnings("boxing")
  public void parseData_AliasesArray() throws IOException {
    byte[] bytes = build(HyperLogLogPlusUniqueStateProto.newBuilder()
        .setData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    CodedInputStream stream = CodedInputStream.newInstance(bytes);
    stream.enableAliasing(true);
    State state = new State();
    state.parse(stream);

    // Preconditions.
    int idx = Bytes.indexOf(bytes, new byte[] {1, 2, 3});
    assertThat(idx).isAtLeast(0);
    assertThat(state.data.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

    // Modify the bytes, make sure that it is reflected in builder.
    bytes[idx + 1] = 4;
    assertThat(state.data.toByteArray()).isEqualTo(new byte[] {1, 4, 3});
  }

  /** Verifies that the data can be parsed also when direct aliasing is not possible. */
  @Test
  public void parseData_ParsesStream() throws IOException {
    byte[] bytes = build(HyperLogLogPlusUniqueStateProto.newBuilder()
        .setData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    State state = new State();
    state.parse(CodedInputStream.newInstance(new ByteArrayInputStream(bytes)));

    assertThat(state.data.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  public void parseSparseData() throws IOException {
    HyperLogLogPlusUniqueStateProto.Builder builder = HyperLogLogPlusUniqueStateProto.newBuilder();
    State state;

    state = parse(builder.clearSparseData());
    assertThat(state.sparseData).isNull();

    state = parse(builder.setSparseData(ByteString.EMPTY));
    assertThat(state.sparseData.toByteArray()).isEmpty();

    state = parse(builder.setSparseData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    assertThat(state.sparseData.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
  }

  /** Verify that the data is aliased when possible. */
  @Test
  @SuppressWarnings("boxing")
  public void parseSparseData_AliasesArray() throws IOException {
    byte[] bytes = build(HyperLogLogPlusUniqueStateProto.newBuilder()
        .setSparseData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    CodedInputStream stream = CodedInputStream.newInstance(bytes);
    stream.enableAliasing(true);
    State state = new State();
    state.parse(stream);

    // Preconditions.
    int idx = Bytes.indexOf(bytes, new byte[] {1, 2, 3});
    assertThat(idx).isAtLeast(0);
    assertThat(state.sparseData.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

    // Modify the bytes, make sure that it is reflected in builder.
    bytes[idx + 1] = 4;
    assertThat(state.sparseData.toByteArray()).isEqualTo(new byte[] {1, 4, 3});
  }

  /** Verifies that the sparse data can be parsed also when aliasing is not possible. */
  @Test
  public void parseSparseData_ParsesStream() throws IOException {
    byte[] bytes = build(HyperLogLogPlusUniqueStateProto.newBuilder()
        .setSparseData(ByteString.copyFrom(new byte[] {1, 2, 3})));
    State state = new State();
    state.parse(CodedInputStream.newInstance(new ByteArrayInputStream(bytes)));

    assertThat(state.sparseData.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  public void parseUnknownField() throws IOException {
    // Create an aggregator state proto with an unknown field in the middle.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    CodedOutputStream coded = CodedOutputStream.newInstance(stream);
    coded.writeInt32(AggregatorStateProto.NUM_VALUES_FIELD_NUMBER, 42);
    coded.writeString(999, "foobar");
    coded.writeInt32(AggregatorStateProto.ENCODING_VERSION_FIELD_NUMBER, 43);
    coded.flush();

    // Check that we can parse the proto, despite the unknown field.
    State state = new State();
    state.parse(CodedInputStream.newInstance(stream.toByteArray()));

    // Check that the fields before and after the unknown fields were correctly read.
    assertEquals(42, state.numValues);
    assertEquals(43, state.encodingVersion);
  }

  @Test
  public void serializeType() throws Exception {
    State state = new State();

    // Required field should always be set.
    AggregatorStateProto actual = serializeAggregatorProto(state);
    assertTrue(actual.hasType());
    assertEquals(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE, actual.getType());

    for (AggregatorType type : AggregatorType.values()) {
      state.type = type;
      actual = serializeAggregatorProto(state);
      assertTrue(actual.hasType());
      assertEquals(type, actual.getType());
    }
  }

  @Test
  public void serializeNumValues() throws Exception {
    State state = new State();

    // Required field should always be set.
    AggregatorStateProto actual = serializeAggregatorProto(state);
    assertTrue(actual.hasNumValues());
    assertEquals(0, actual.getNumValues());

    state.numValues = 42;
    actual = serializeAggregatorProto(state);
    assertTrue(actual.hasNumValues());
    assertEquals(42, actual.getNumValues());
  }

  @Test
  public void serializeEncodingVersion() throws Exception {
    State state = new State();

    AggregatorStateProto actual = serializeAggregatorProto(state);
    assertFalse(actual.hasEncodingVersion());

    state.encodingVersion = 2;
    actual = serializeAggregatorProto(state);
    assertTrue(actual.hasEncodingVersion());
    assertEquals(2, actual.getEncodingVersion());
  }

  @Test
  @SuppressWarnings("boxing")
  public void serializeValueType() throws Exception {
    State state = new State();

    AggregatorStateProto actual = serializeAggregatorProto(state);
    assertThat(actual.hasValueType()).isFalse();

    for (DefaultOpsType.Id opsType : DefaultOpsType.Id.values()) {
      ValueType valueType = ValueType.forStandardType(opsType);
      state.valueType = valueType;
      actual = serializeAggregatorProto(state);
      if (valueType.equals(ValueType.UNKNOWN)) {
        assertThat(actual.hasValueType()).isFalse();
      }
      assertThat(ValueType.forStateProto(actual)).isEqualTo(valueType);
    }
  }

  @Test
  public void serializeSparseSize() throws Exception {
    State state = new State();

    HyperLogLogPlusUniqueStateProto actual = serializeHllProto(state);
    assertFalse(actual.hasSparseSize());

    state.sparseSize = 42;
    actual = serializeHllProto(state);
    assertEquals(42, actual.getSparseSize());
    assertTrue(actual.hasSparseSize());
  }

  @Test
  public void serializePrecision() throws Exception {
    State state = new State();

    HyperLogLogPlusUniqueStateProto actual = serializeHllProto(state);
    assertFalse(actual.hasPrecisionOrNumBuckets());

    state.precision = 42;
    actual = serializeHllProto(state);
    assertEquals(42, actual.getPrecisionOrNumBuckets());
    assertTrue(actual.hasPrecisionOrNumBuckets());
  }

  @Test
  public void serializeSparsePrecision() throws Exception {
    State state = new State();

    HyperLogLogPlusUniqueStateProto actual = serializeHllProto(state);
    assertFalse(actual.hasSparsePrecisionOrNumBuckets());

    state.sparsePrecision = 42;
    actual = serializeHllProto(state);
    assertEquals(42, actual.getSparsePrecisionOrNumBuckets());
    assertTrue(actual.hasSparsePrecisionOrNumBuckets());
  }

  @Test
  public void serializeData() throws Exception {
    State state = new State();

    HyperLogLogPlusUniqueStateProto actual = serializeHllProto(state);
    assertFalse(actual.hasData());

    state.data = ByteSlice.copyOnWrite(new byte[] { 1, 2, 3 });
    actual = serializeHllProto(state);
    assertThat(actual.getData().toByteArray()).isEqualTo(new byte[] { 1, 2, 3 });
  }

  @Test
  public void serializeSparseData() throws Exception {
    State state = new State();

    HyperLogLogPlusUniqueStateProto actual = serializeHllProto(state);
    assertFalse(actual.hasSparseData());

    state.sparseData = GrowingByteSlice.copyOnWrite(new byte[] { 1, 2, 3 });
    actual = serializeHllProto(state);
    assertThat(actual.getSparseData().toByteArray()).isEqualTo(new byte[] { 1, 2, 3 });
  }

  /** Returns a {@link State}, parsed from the given proto. */
  private static State parse(AggregatorStateProto.Builder builder) throws IOException {
    State state = new State();
    state.parse(builder.buildPartial().toByteString().newCodedInput());
    return state;
  }

  /**
   * Returns a {@link State}, parsed from a default {@link AggregatorStateProto} of the given {@link
   * HyperLogLogPlusUniqueStateProto}.
   */
  private static State parse(HyperLogLogPlusUniqueStateProto.Builder builder) throws IOException {
    State state = new State();
    state.parse(CodedInputStream.newInstance(build(builder)));
    return state;
  }

  /**
   * Wraps a {@link HyperLogLogPlusUniqueStateProto} into a default {@link AggregatorStateProto} and
   * returns its serialized bytes.
   */
  private static byte[] build(HyperLogLogPlusUniqueStateProto.Builder builder) {
    return AggregatorStateProto.newBuilder()
        .setType(AggregatorType.HYPERLOGLOG_PLUS_UNIQUE)
        .setNumValues(0)
        .setExtension(HllplusUnique.hyperloglogplusUniqueState, builder.buildPartial())
        .buildPartial()
        .toByteArray();
  }

  private static AggregatorStateProto serializeAggregatorProto(State state)
      throws InvalidProtocolBufferException {
    ExtensionRegistryLite registry = ExtensionRegistryLite.newInstance();
    HllplusUnique.registerAllExtensions(registry);
    return AggregatorStateProto.parseFrom(state.toByteArray(), registry);
  }

  private static HyperLogLogPlusUniqueStateProto serializeHllProto(State state)
      throws InvalidProtocolBufferException {
    return serializeAggregatorProto(state).getExtension(HllplusUnique.hyperloglogplusUniqueState);
  }
}
