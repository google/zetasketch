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

package com.google.zetasketch.internal.hash;

import static com.google.common.hash.Hashing.fingerprint2011;

import java.nio.charset.StandardCharsets;

/**
 * Library to compute standard 64 bit hashes for values.
 *
 * <p>This library is designed to return specific hashes that are compatible with other programming
 * languages, in particular C++ and Go. This is important so that aggregators that use hashes
 * produce {@code AggregatorStateProto}s that are exchangeable between different implementations.
 */
public final class Hash {

  /** Returns the 64 bit hash of the byte array value. */
  public static long of(byte[] value) {
    return fingerprint2011().hashBytes(value).asLong();
  }

  /** Returns the 64 bit hash of the integer value. */
  public static long of(int value) {
    return fingerprint2011().hashInt(value).asLong();
  }

  /** Returns the 64 bit hash of the long value. */
  public static long of(long value) {
    return fingerprint2011().hashLong(value).asLong();
  }

  /** Returns the 64 bit hash of the String value. */
  public static long of(String value) {
    return fingerprint2011().hashString(value, StandardCharsets.UTF_8).asLong();
  }

  // Utility class.
  private Hash() {}
}
