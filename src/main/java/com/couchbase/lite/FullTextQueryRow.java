package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor Bonnet on 07/01/15.
 */
public class FullTextQueryRow extends QueryRow {

    private String docId;
    private long sequence;
    private int fullTextId;
    private String offsets;

    public FullTextQueryRow(String docId, long sequence, int fullTextId, String offsets, Object value) {
        super(docId, sequence, null, value, null);
        this.docId = docId;
        this.sequence = sequence;
        this.fullTextId = fullTextId;
        this.offsets = offsets;
    }


    @InterfaceAudience.Private
    public Map<String, Object> asJSONDictionary() {
        Map<String, Object> result = new HashMap<String, Object>();
        if (value != null || sourceDocumentId != null) {
            if (value != null){
                result.put("value", value);
            }
            result.put("id", sourceDocumentId);
            if (documentProperties != null){
                result.put("doc", documentProperties);
            }
        } else {
            result.put("key", key);
            result.put("error", "not_found");
        }
        return result;
    }
}
