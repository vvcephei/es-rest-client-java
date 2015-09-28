package org.elasticsearch.indices;

import org.elasticsearch.index.Index;

import java.util.Map;

/**
 * An extension of IndexMissingException that also holds onto the map of
 * response values from the REST call for debugging/extra info.
 */
public class RestIndexMissingException extends IndexMissingException {

    private Map<String, Object> repsonseMap;

    public RestIndexMissingException(Index index, Map<String, Object> responseMap) {
        super(index);
        this.repsonseMap = responseMap;
    }

    public Map<String, Object> getRepsonseMap() {
        return repsonseMap;
    }
}
