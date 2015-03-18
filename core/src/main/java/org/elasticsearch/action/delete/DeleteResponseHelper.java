package org.elasticsearch.action.delete;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireBoolean;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireLong;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;

public class DeleteResponseHelper {
    public static DeleteResponse fromXContent(final Map<String, Object> map) {
        return new DeleteResponse(
            requireString(map.get("_index")),
            requireString(map.get("_type")),
            requireString(map.get("_id")),
            requireLong(map.get("_version")),
            requireBoolean(map.get("found")));
    }
}
