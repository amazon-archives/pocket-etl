/*
 *   Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.pocketEtl.extractor;

import org.joda.time.DateTime;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TestDTO {
    private int aInt;
    private String aString;
    private double aDouble;
    private boolean aBoolean;
    private DateTime aDateTime;
    private Instant java8DateTime;
    private List<String> aList;
    private Optional<String> optionalString;

    public TestDTO() {
        // no-op
    }

    public TestDTO(int aInt, String aString, double aDouble, boolean aBoolean, DateTime aDateTime, Instant java8DateTime,
                   List<String> aList, Optional<String> optionalString
    ) {
        this.aInt = aInt;
        this.aString = aString;
        this.aDouble = aDouble;
        this.aBoolean = aBoolean;
        this.aDateTime = aDateTime;
        this.java8DateTime = java8DateTime;
        this.aList = aList;
        this.optionalString = optionalString;
    }

    public int getAInt() {
        return aInt;
    }

    public void setAInt(int aInt) {
        this.aInt = aInt;
    }

    public String getAString() {
        return aString;
    }

    public void setAString(String aString) {
        this.aString = aString;
    }

    public double getADouble() {
        return aDouble;
    }

    public void setADouble(double aDouble) {
        this.aDouble = aDouble;
    }

    public boolean isABoolean() {
        return aBoolean;
    }

    public void setABoolean(boolean aBoolean) {
        this.aBoolean = aBoolean;
    }

    public DateTime getADateTime() {
        return aDateTime;
    }

    public void setADateTime(DateTime aDateTime) {
        this.aDateTime = aDateTime;
    }

    public Instant getJava8DateTime() {
        return java8DateTime;
    }

    public void setJava8DateTime(Instant java8DateTime) {
        this.java8DateTime = java8DateTime;
    }

    public List<String> getAList() {
        return aList;
    }

    public void setAList(List<String> aList) {
        this.aList = aList;
    }

    public Optional<String> getOptionalString() {
        return optionalString;
    }

    public void setOptionalString(Optional<String> optionalString) {
        this.optionalString = optionalString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestDTO testDTO = (TestDTO) o;
        return aInt == testDTO.aInt &&
                Double.compare(testDTO.aDouble, aDouble) == 0 &&
                aBoolean == testDTO.aBoolean &&
                Objects.equals(aString, testDTO.aString) &&
                Objects.equals(aDateTime, testDTO.aDateTime) &&
                Objects.equals(java8DateTime, testDTO.java8DateTime) &&
                Objects.equals(aList, testDTO.aList) &&
                Objects.equals(optionalString, testDTO.optionalString);
    }

    @Override
    public String toString() {
        return "TestDTO{" +
                "aInt=" + aInt +
                ", aString='" + aString + '\'' +
                ", aDouble=" + aDouble +
                ", aBoolean=" + aBoolean +
                ", aDateTime=" + aDateTime +
                ", java8DateTime=" + java8DateTime +
                ", aList=" + aList +
                ", optionalString=" + optionalString +
                '}';
    }
}
