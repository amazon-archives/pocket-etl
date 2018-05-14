/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.transformer;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class MapTransformerTest {
    private MapTransformer<String, String> mapTransformer = MapTransformer.of(String::toLowerCase);

    @Test
    public void transformCanTransformAnObject() {
        List<String> result = mapTransformer.transform("TEST");

        assertThat(result, equalTo(ImmutableList.of("test")));
    }
}
