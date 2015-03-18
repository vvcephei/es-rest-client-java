package org.elasticsearch.action.get;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.readBytesReference;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class GetHelper {
    public static GetResponse fromXContent(final Map<String, Object> map) {
        final Map<String, GetField> fields;
        if (map.containsKey("fields")) {
            Map<String, Object> incoming = requireMap(map.get("fields"), String.class, Object.class);
            fields = Maps.newHashMapWithExpectedSize(incoming.size());
            for (Map.Entry<String, Object> entry : incoming.entrySet()) {
                if (entry.getValue() instanceof List) {
                    fields.put(entry.getKey(), new GetField(entry.getKey(), requireList(entry.getValue(), Object.class)));
                } else {
                    fields.put(entry.getKey(), new GetField(entry.getKey(), ImmutableList.of(entry.getValue())));
                }
            }
        } else {
            fields = ImmutableMap.of();
        }

        return new GetResponse(new GetResult(
            requireString(map.get("_index")),
            requireString(map.get("_type")),
            requireString(map.get("_id")),
            nodeLongValue(map.get("_version"), -1),
            nodeBooleanValue(map.get("found"), true),
            readBytesReference(map.get("_source")),
            fields
        ));
    }
}
