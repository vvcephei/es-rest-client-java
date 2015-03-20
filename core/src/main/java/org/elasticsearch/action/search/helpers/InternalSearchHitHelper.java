package org.elasticsearch.action.search.helpers;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeBytesReferenceValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class InternalSearchHitHelper {
    public static InternalSearchHit fromXContent(final Map<String, Object> map) {
        Object explanation = map.get("_explanation");
        String nodeid = null;
        // FIXME TO_PR not quite right, but the es serialization node is confusing. see https://github.com/bazaarvoice/es-client-java/issues/8
        int shardid = -1;
        // it only serializes _shard and _node if explanation != null, but it always serializes _index,
        // and the only way _index could be set is at the same time as the other fields,
        // (unless it comes from the read(instream) method, in which case shardid and index get set, but nodeid may be unset.
        // which suggests that at least index and shardid are set on the ES side, but they are deliberately
        // leaving shardid off from the serialization when explanation() is null.
        // If so, there is no way I can recover that information, so I'll just set shardId to an impossible value
        // TODO send a PR to elasticsearch to fix this on the server side
        if (explanation != null) {
            shardid = nodeIntegerValue(map.get("_shard"));
            nodeid = nodeStringValue(map.get("_node"), null);
        }
        String index = nodeStringValue(map.get("_index"), null);
        SearchShardTarget searchShardTarget = new SearchShardTarget(nodeid, index, shardid);
        Text type = new StringText(nodeStringValue(map.get("_type"), null));
        String id = nodeStringValue(map.get("_id"), null);
        long version = nodeLongValue(map.get("_version"), -1);
        float score = nodeFloatValue(map.get("_score"), Float.NaN);
        BytesReference source = nodeBytesReferenceValue(map.get("_source"));
        final int docId = -1; // this field isn't serialized
        ImmutableMap.Builder<String, SearchHitField> fields = ImmutableMap.builder();
        if (map.containsKey("fields")) {
            Map<String, Object> fieldsMap = nodeMapValue(map.get("fields"), String.class, Object.class);
            for (Map.Entry<String, Object> fieldEntry : fieldsMap.entrySet()) {
                final ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
                if (fieldEntry.getValue() instanceof List) {
                    for (Object value : nodeListValue(fieldEntry.getValue(), Object.class)) {
                        valuesBuilder.add(value);
                    }
                } else {
                    valuesBuilder.add(fieldEntry.getValue());
                }
                SearchHitField field = new InternalSearchHitField(fieldEntry.getKey(), valuesBuilder.build());
                fields.put(field.getName(), field);
            }
        }
        InternalSearchHit internalSearchHit = new InternalSearchHit(docId, id, type, fields.build());
        internalSearchHit.sourceRef(source);

        internalSearchHit.shardTarget(searchShardTarget);

        if (map.containsKey("highlight")) {
            ImmutableMap.Builder<String, HighlightField> highlights = ImmutableMap.builder();
            final Map<String, Object> highlightMap = nodeMapValue(map.get("highlight"), String.class, Object.class);
            for (Map.Entry<String, Object> entry : highlightMap.entrySet()) {
                final String name = entry.getKey();
                final Text[] fragments;
                if (entry.getValue() == null) {
                    fragments = null;
                } else {
                    final List<String> strings = nodeListValue(entry.getValue(), String.class);
                    fragments = new Text[strings.size()];
                    for (int i = 0; i < strings.size(); i++) {
                        fragments[i] = new StringText(strings.get(i));
                    }
                }
                final HighlightField highlightField = new HighlightField(name, fragments);
                highlights.put(highlightField.getName(), highlightField);
            }
            internalSearchHit.highlightFields(highlights.build());
        }

        if (map.containsKey("sort")) {
            // TODO: can't really tell if this is the right thing to do.
            final List<Object> sorts = nodeListValue(map.get("sort"), Object.class);
            internalSearchHit.sortValues(sorts.toArray());
        }

        if (map.containsKey("matched_filters")) {
            final List<String> matched_filters = nodeListValue(map.get("matched_filters"), String.class);
            internalSearchHit.matchedQueries(matched_filters.toArray(new String[matched_filters.size()]));
        }

        if (explanation != null) {
            internalSearchHit.explanation(getExplanation(explanation));
        }

        return internalSearchHit;
    }

    private static Explanation getExplanation(final Object explanationObj) {
        final Map<String, Object> explainMap = nodeMapValue(explanationObj, String.class, Object.class);
        final float value = nodeFloatValue(explainMap.get("value"));
        final String description = nodeStringValue(explainMap.get("description"), null);
        final Explanation explanation = new Explanation(value, description);
        if (explainMap.containsKey("details")) {
            for (Object detail : nodeListValue(explainMap.get("details"), Object.class)) {
                explanation.addDetail(getExplanation(detail));
            }
        }
        return explanation;
    }
}
