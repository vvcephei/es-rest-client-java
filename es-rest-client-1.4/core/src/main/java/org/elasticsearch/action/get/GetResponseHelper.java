package org.elasticsearch.action.get;

import org.elasticsearch.action.FromXContent;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeBytesReferenceForMapValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeStringValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

/**
 * The inverse of {@link GetResponse#toXContent(org.elasticsearch.common.xcontent.XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)}
 */
public class GetResponseHelper implements FromXContent<GetResponse> {
    @Override public GetResponse fromXContent(final Map<String, Object> map) {
        final Map<String, GetField> fields;
        if (map.containsKey("fields")) {
            Map<String, Object> incoming = nodeMapValue(map.get("fields"), String.class, Object.class);
            fields = Maps.newHashMapWithExpectedSize(incoming.size());
            for (Map.Entry<String, Object> entry : incoming.entrySet()) {
                if (entry.getValue() instanceof List) {
                    fields.put(entry.getKey(), new GetField(entry.getKey(), nodeListValue(entry.getValue(), Object.class)));
                } else {
                    fields.put(entry.getKey(), new GetField(entry.getKey(), ImmutableList.of(entry.getValue())));
                }
            }
        } else {
            fields = ImmutableMap.of();
        }

        //noinspection unchecked
        return new GetResponse(new GetResult(
            nodeStringValue(map.get("_index")),
            nodeStringValue(map.get("_type")),
            nodeStringValue(map.get("_id")),
            nodeLongValue(map.get("_version"), -1),
            nodeBooleanValue(map.get("found"), true),
            nodeBytesReferenceForMapValue((Map<String, ?>) map.get("_source")),
            fields
        ));
    }
}
