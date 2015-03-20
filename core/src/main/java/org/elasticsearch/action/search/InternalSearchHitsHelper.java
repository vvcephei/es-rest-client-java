package org.elasticsearch.action.search;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalSearchHitsHelper {
    public static InternalSearchHits fromXContent(final Map<String, Object> map) {
        if (!map.containsKey("hits")) {
            return null;
        } else {
            Map<String, Object> hitsMap = requireMap(map.get("hits"), String.class, Object.class);
            final long totalHits = nodeLongValue(hitsMap.get("total"));
            final float maxScore = hitsMap.get("max_score") != null ? nodeFloatValue(hitsMap.get("max_score")) : Float.NaN;

            List<InternalSearchHit> internalSearchHits = Lists.newArrayList();
            if (hitsMap.containsKey("hits")) {
                List<Object> hitsList = requireList(hitsMap.get("hits"), Object.class);
                for (Object hit : hitsList) {
                    internalSearchHits.add(InternalSearchHitHelper.fromXContent(requireMap(hit, String.class, Object.class)));

                }
            }
            return new InternalSearchHits(internalSearchHits.toArray(new InternalSearchHit[internalSearchHits.size()]), totalHits, maxScore);
        }
    }


}
