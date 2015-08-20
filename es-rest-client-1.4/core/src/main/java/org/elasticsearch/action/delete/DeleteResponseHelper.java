package org.elasticsearch.action.delete;

import org.elasticsearch.action.FromXContent;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeStringValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

/**
 * The inverse of the anonymous {@link org.elasticsearch.rest.action.support.RestBuilderListener}
 * in {@link org.elasticsearch.rest.action.delete.RestDeleteAction}
 */
public class DeleteResponseHelper implements FromXContent<DeleteResponse>{
    @Override public DeleteResponse fromXContent(final Map<String, Object> map) {
        return new DeleteResponse(
            nodeStringValue(map.get("_index")),
            nodeStringValue(map.get("_type")),
            nodeStringValue(map.get("_id")),
            nodeLongValue(map.get("_version")),
            nodeBooleanValue(map.get("found")));
    }
}
