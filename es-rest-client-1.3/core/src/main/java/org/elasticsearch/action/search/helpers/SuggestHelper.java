package org.elasticsearch.action.search.helpers;

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

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeStringValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SuggestHelper {
    public static Suggest fromXContent(final Map<String, Object> map) {
        // weirdly, the documentation claims that
        // the suggestions will be in a "suggest" field,
        // but I don't see that being written in the 1.4.4 code.
        // my hypothesis is that the "name" (below) of the top-level
        // Suggest is "suggest".
        if (!map.containsKey("suggest")) {
            return null;
        }
        Map<String, Object> suggest = nodeMapValue(map.get("suggest"), String.class, Object.class);
        if (suggest.isEmpty()) {
            return new Suggest(ImmutableList.<Suggestion<? extends Entry<? extends Option>>>of());
        }

        // suggest is not empty so we can do this
        Map.Entry<String, Object> first = first(suggest);
        if (first.getValue() instanceof List) {
            // then the suggest object was anonymous, and we need to iterate over the suggestions
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = Lists.newArrayList();
            Map<String, Object> namedSuggestions = nodeMapValue(suggest, String.class, Object.class);
            for (Map.Entry<String, Object> namedSuggestion : namedSuggestions.entrySet()) {
                suggestions.add(SuggestionHelper.fromXContent(namedSuggestion.getKey(), nodeListValue(namedSuggestion.getValue(), Object.class)));
            }
            return new Suggest(null, suggestions);
        } else {
            Preconditions.checkState(suggest.size() == 1);
            // then the suggest object had a name
            final String name = first.getKey();
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = Lists.newArrayList();
            Map<String, Object> namedSuggestions = nodeMapValue(first.getValue(), String.class, Object.class);
            for (Map.Entry<String, Object> namedSuggestion : namedSuggestions.entrySet()) {
                suggestions.add(SuggestionHelper.fromXContent(namedSuggestion.getKey(), nodeListValue(namedSuggestion.getValue(), Object.class)));
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
                suggestion.addTerm(EntryHelper.fromXContent(nodeMapValue(entryObj, String.class, Object.class)));
            }
            return suggestion;

        }
    }

    private static class EntryHelper {
        public static Entry<? extends Option> fromXContent(final Map<String, Object> map) {
            Entry<Option> entry = new Entry<Option>(new StringText(nodeStringValue(map.get("text"))), nodeIntegerValue(map.get("offset")), nodeIntegerValue(map.get("length")));
            List<Object> optionObjs = nodeListValue(map.get("options"), Object.class);
            for (Object optionObj : optionObjs) {
                entry.addOption(OptionHelper.fromXContent(nodeMapValue(optionObj, String.class, Object.class)));
            }
            return entry;
        }
    }

    private static class OptionHelper {

        public static Option fromXContent(final Map<String, Object> map) {
            final String text = nodeStringValue(map.get("text"));
            final /*nullable*/ String highlighted = nodeStringValue(map.get("highlighted"), null);
            final float score = nodeFloatValue(map.get("score"));
            return new Option(new StringText(text), new StringText(highlighted), score);
        }
    }
}
