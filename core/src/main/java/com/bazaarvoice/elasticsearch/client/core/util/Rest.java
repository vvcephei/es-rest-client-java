package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.Iterables;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;

public class Rest {
    public static RestStatus findRestStatus(final int status) {
        return Iterables.find(Arrays.asList(RestStatus.values()), new Predicate<RestStatus>() {
            @Override public boolean apply(final RestStatus restStatus) {
                return restStatus.getStatus() == status;
            }
        });
    }
}
