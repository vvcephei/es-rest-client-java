package org.elasticsearch;

import org.elasticsearch.index.Index;
import org.elasticsearch.indices.RestIndexMissingException;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Responsible for constructing/extracting the proper exception from an ElasticSearch
 * REST call response when there is an error condition.
 */
public final class ElasticSearchExceptionHelper {

    public static final Pattern INDEX_MISSING_EX_PATTERN = Pattern.compile("^IndexMissingException\\[\\[((.*))\\] missing\\]$");

    private ElasticSearchExceptionHelper() {
    }

    /**
     * Gets the appropriate exception from a result map from a RESTful call. The map is
     * expected to have an entry for 'error' that described the error that occurred.
     *
     * Currently only supports special handling for IndexMissingExceptions. If the
     * cause is an IndexMissingException then that will be returned otherwise the
     * error message will be wrapped with a RuntimeException and returned.
     *
     * @param map non-null result map from RESTful call
     * @return proper exception; IndexMissingException or RuntimeException with error message
     *          in the 'error' map entry.
     */
    public static RuntimeException getProperException(Map<String, Object> map) {

        // TODO add support for other ES exceptions - see https://github.com/bazaarvoice/es-client-java/issues/3

        String errorMessage = Objects.toString(map.get("error"));

        Matcher matcher;
        if ((matcher = INDEX_MISSING_EX_PATTERN.matcher(errorMessage)).matches()) {
            return new RestIndexMissingException(new Index(matcher.group(1)), map);
        //} else if ((matcher = SOME_OTHER_EX_PATTERN.matcher(errorMessage)).matches()) {
        //      return new SomeOtherElasticSearchException(...)
        } else {
            return new RuntimeException("Encountered error: " + Objects.toString(map));
        }
    }
}
