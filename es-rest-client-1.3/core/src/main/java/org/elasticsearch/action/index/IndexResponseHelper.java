package org.elasticsearch.action.index;

import org.elasticsearch.action.FromXContent;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeStringValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

/**
 * The inverse of the anonymous {@link org.elasticsearch.rest.action.support.RestBuilderListener}
 * in {@link org.elasticsearch.rest.action.index.RestIndexAction}
 */
public class IndexResponseHelper implements FromXContent<IndexResponse> {
    @Override public IndexResponse fromXContent(final Map<String, Object> map) {
        return new IndexResponse(
            nodeStringValue(map.get("_index")),
            nodeStringValue(map.get("_type")),
            nodeStringValue(map.get("_id")),
            nodeLongValue(map.get("_version")),
            nodeBooleanValue(map.get("created")));
    }
}
