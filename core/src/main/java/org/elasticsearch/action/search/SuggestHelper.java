package org.elasticsearch.action.search;

import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireFloat;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireInt;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SuggestHelper {
    public static Suggest fromXContent(final Map<String, Object> map) {
        // weirdly, the suggestions will be in a "suggest" field,
        // but I don't see that being written in the 1.4.4 code
        if (!map.containsKey("suggest")) {
            return null;
        }
        Map<String, Object> suggest = requireMap(map.get("suggest"), String.class, Object.class);
        if (suggest.isEmpty()) {
            return new Suggest(ImmutableList.<Suggestion<? extends Entry<? extends Option>>>of());
        }

        // suggest is not empty so we can do this
        Map.Entry<String, Object> first = first(suggest);
        if (first.getValue() instanceof List) {
            // then the suggest object was anonymous, and we need to iterate over the suggestions
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = Lists.newArrayList();
            Map<String, Object> namedSuggestions = requireMap(suggest, String.class, Object.class);
            for (Map.Entry<String, Object> namedSuggestion : namedSuggestions.entrySet()) {
                suggestions.add(SuggestionHelper.fromXContent(namedSuggestion.getKey(), requireList(namedSuggestion.getValue(), Object.class)));
            }
            return new Suggest(null, suggestions);
        } else {
            Preconditions.checkState(suggest.size() == 1);
            // then the suggest object had a name
            final String name = first.getKey();
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = Lists.newArrayList();
            Map<String, Object> namedSuggestions = requireMap(first.getValue(), String.class, Object.class);
            for (Map.Entry<String, Object> namedSuggestion : namedSuggestions.entrySet()) {
                suggestions.add(SuggestionHelper.fromXContent(namedSuggestion.getKey(), requireList(namedSuggestion.getValue(), Object.class)));
            }
            return new Suggest(new XContentBuilderString(name), suggestions);
        }
    }

    private static <K, V> Map.Entry<K, V> first(final Map<K, V> map) {
        Preconditions.checkArgument(!map.isEmpty());
        return map.entrySet().iterator().next();
    }

    private static class SuggestionHelper {
        public static Suggestion<? extends Entry<? extends Option>> fromXContent(final String name, final List<Object> entryObjs) {
            Suggestion<Entry<? extends Option>> suggestion = new Suggestion<Entry<? extends Option>>(name, -1/*Internal field. not needed.*/);
            for (Object entryObj : entryObjs) {
                suggestion.addTerm(EntryHelper.fromXContent(requireMap(entryObj, String.class, Object.class)));
            }
            return suggestion;

        }
    }

    private static class EntryHelper {
        public static Entry<? extends Option> fromXContent(final Map<String, Object> map) {
            Entry<Option> entry = new Entry<Option>(new StringText(requireString(map.get("text"))), requireInt(map.get("offset")), requireInt(map.get("length")));
            List<Object> optionObjs = requireList(map.get("options"), Object.class);
            for (Object optionObj : optionObjs) {
                entry.addOption(OptionHelper.fromXContent(requireMap(optionObj, String.class, Object.class)));
            }
            return entry;
        }
    }

    private static class OptionHelper {

        public static Option fromXContent(final Map<String, Object> map) {
            final String text = requireString(map.get("text"));
            final /*nullable*/ String highlighted = nodeStringValue(map.get("highlighted"), null);
            final float score = requireFloat(map.get("score"));
            final /*nullable*/ Boolean collateMatch = map.containsKey("collate_match") ? nodeBooleanValue(map.get("collate_match")) : null;
            return new Option(new StringText(text), new StringText(highlighted), score, collateMatch);
        }
    }
}
