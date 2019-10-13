/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.util.Objects;

import org.apache.arrow.flight.impl.Flight;

/**
 * POJO wrapper around protocol specifics for Flight actions.
 */
public final class ActionType {
  private final String type;
  private final String description;

  /**
   * Construct a new instance.
   *
   * @param type The type of action to perform
   * @param description The description of the type.
   */
  public ActionType(String type, String description) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(description, "description must not be null");
    this.type = type;
    this.description = description;
  }

  /**
   * Constructs a new instance from the corresponding protocol buffer object.
   */
  ActionType(Flight.ActionType type) {
    this.type = type.getType();
    this.description = type.getDescription();
  }

  public String getType() {
    return type;
  }

  /**
   *  Converts the POJO to the corresponding protocol buffer type.
   */
  Flight.ActionType toProtocol() {
    return Flight.ActionType.newBuilder()
        .setType(type)
        .setDescription(description)
        .build();
  }

  @Override
  public String toString() {
    return "ActionType{" +
        "type='" + type + '\'' +
        ", description='" + description + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ActionType that = (ActionType) o;
    return type.equals(that.type) &&
        description.equals(that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, description);
  }
}
