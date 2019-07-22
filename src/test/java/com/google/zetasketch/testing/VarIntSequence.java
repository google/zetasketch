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

import com.google.zetasketch.internal.VarInt;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Utility class for writing a series of variably encoded integers in tests. */
public class VarIntSequence {

  public static byte[] varints(int... values) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    for (int value : values) {
      try {
        VarInt.putVarInt(value, buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return buffer.toByteArray();
  }

  // Utility class.
  private VarIntSequence() {}
}
