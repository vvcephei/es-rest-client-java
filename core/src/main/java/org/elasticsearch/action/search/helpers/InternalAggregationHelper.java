package org.elasticsearch.action.search.helpers;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsHelper;

import java.util.Map;

public class InternalAggregationHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map) {

        //doubleTerms first
        doubleTermsFromXContent(name, map);


        return null;
    }

    private static DoubleTerms doubleTermsFromXContent(final String name, final Map<String, Object> map) {return DoubleTermsHelper.fromXContent(name, map);}
}
