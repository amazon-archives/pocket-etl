package com.amazon.pocketEtl.core;

import lombok.EqualsAndHashCode;

import java.util.function.Function;

/**
 * This Logging Strategy is applied if no Logging Strategy was provided by the user.
 * @param <T> Type of the object to be logged
 */
@EqualsAndHashCode
public class DefaultLoggingStrategy<T> implements Function<T, String> {

    private static final String DEFAULT_LOG_MESSAGE = "For more detailed logging for the object that failed, provide a "
            + "custom logging strategy to the EtlStage.";

    @Override
    public String apply(T t) {
        return DEFAULT_LOG_MESSAGE;
    }
}
