/*
 * Copyright © 2019 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.proto;

import static org.reflections.ReflectionUtils.getAllSuperTypes;
import static org.reflections.ReflectionUtils.getMethods;

import io.camunda.zeebe.protocol.record.JsonSerializable;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RecordValueWithVariables;
import io.camunda.zeebe.protocol.record.ValueType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

/**
 * Helper class to detect missing Zeebe Record ValueTypes and their missing attributes. Will just
 * print the results to console and test will not fail.
 */
public class DetectMissingFeaturesTest {

  @Test
  public void findMissingRecordValueTypeFeatures() throws Exception {
    // prepare: get all Protobuf message classes
    Reflections reflections1 =
        new Reflections("io.zeebe.exporter.proto", new SubTypesScanner(false));
    var protobufMessages = reflections1.getSubTypesOf(com.google.protobuf.GeneratedMessageV3.class);

    Reflections reflections =
        new Reflections("io.camunda.zeebe.protocol.record.value", new SubTypesScanner(false));
    reflections.getSubTypesOf(RecordValue.class).stream()
        .filter(
            c ->
                !(c.equals(RecordValueWithVariables.class)
                    || c.getSimpleName().startsWith("Immutable")))
        .forEach(
            c1 -> {
              System.out.println("Testing " + c1.getName());
              Set<Class<?>> superTypes = getAllSuperTypes(c1);
              superTypes.remove(RecordValue.class);
              superTypes.remove(JsonSerializable.class);
              superTypes.remove(c1);
              Set<Method> methods = getMethods(c1);

              // list type hierarchy
              superTypes.forEach(st -> {});
              // filter all relevant methods
              var relevantMethods =
                  methods.stream()
                      .filter(
                          m ->
                              m.getDeclaringClass().equals(c1)
                                  || superTypes.contains(m.getDeclaringClass()))
                      .collect(Collectors.toList());

              // check that corresponding ProtoBuf class exists
              var zeebeName =
                  c1.getSimpleName().endsWith("Value")
                      ? c1.getSimpleName()
                          .substring(0, c1.getSimpleName().length() - "Value".length())
                      : c1.getSimpleName();
              var protobufName = "io.zeebe.exporter.proto.Schema$" + zeebeName;
              var protobufClass =
                  protobufMessages.stream()
                      .filter(pm -> pm.getName().equals(protobufName))
                      .findFirst();
              if (protobufClass.isEmpty()) {
                // check if protobuf class with suffix "Record" exists
                if (!protobufName.endsWith("Record"))
                  protobufClass =
                      protobufMessages.stream()
                          .filter(pm -> pm.getName().equals(protobufName + "Record"))
                          .findFirst();

                if (protobufClass.isEmpty()) {
                  // check if protobuf class deeper in hierarchy exists
                  protobufClass =
                      protobufMessages.stream()
                          .filter(pm -> pm.getName().endsWith("$" + zeebeName))
                          .findFirst();
                  if (protobufClass.isEmpty()) {
                    // check if there is a Zeebe ValueType for potentially missing class
                    var valueTypeName = zeebeName;
                    if (valueTypeName.endsWith("Record"))
                      valueTypeName =
                          valueTypeName.substring(0, valueTypeName.length() - "Record".length());
                    valueTypeName =
                        valueTypeName
                            .replaceAll("([A-Z])(?=[A-Z])", "$1_")
                            .replaceAll("([a-z])([A-Z])", "$1_$2")
                            .toUpperCase();
                    try {
                      ValueType.valueOf(valueTypeName);
                      // Zeebe ValueType exists - so we should have a protobuf representation
                      System.err.println(
                          "=> Missing protobuf class for "
                              + c1.getSimpleName()
                              + " !! -> ValueType."
                              + valueTypeName);
                    } catch (IllegalArgumentException e) {
                      // doesn't exist as Zeebe ValueType -> don't bother
                    }
                    return;
                  }
                }
              }
              if (c1.isAnnotationPresent(Deprecated.class)) {
                System.err.println(
                    "=> Implementing protobuf class for deprecated " + c1.getSimpleName());
              }

              // compare methods of original Zeebe Record with corresponding protobuf class
              var theProtobufClass = protobufClass.get();
              System.out.println(
                  "Analyzing corresponding generated protobuf class " + theProtobufClass.getName());
              Set<Method> protobufMethods = getMethods(theProtobufClass);
              relevantMethods.forEach(
                  rm -> {
                    var name = rm.getName();
                    String originalName = name;
                    if (List.class.isAssignableFrom(rm.getReturnType())) name = name + "List";
                    if (name.startsWith("is")) name = name.replaceFirst("is", "get");
                    String getName = name;
                    String getIsName = rm.getName().replaceFirst("is", "getIs");
                    boolean missing = false;
                    if (protobufMethods.stream()
                        .filter(
                            pm ->
                                pm.getName().equals(getName)
                                    || pm.getName().equals(getIsName)
                                    || pm.getName().equals(originalName))
                        .findFirst()
                        .isEmpty()) {
                      // will not implement already deprecated stuff
                      if (!rm.isAnnotationPresent(Deprecated.class))
                        System.err.println(
                            "-> Missing protobuf attribute for "
                                + theProtobufClass.getSimpleName()
                                + "::"
                                + rm.getName());
                      missing = true;
                    }
                    if (!missing && rm.isAnnotationPresent(Deprecated.class)) {
                      System.err.println(
                          "-> Deprecated protobuf method "
                              + theProtobufClass.getSimpleName()
                              + "::"
                              + rm.getName());
                    }
                  });
            });
  }
}
