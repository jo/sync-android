/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.common.SimpleChangeNotifyingMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>A single revision of a document within a datastore.</p>
 *
 * <p>Documents within the datastore are in fact trees of document revisions,
 * with one document marked as the current winner at any point. Branches in
 * the tree are caused when a document is edited in more than one place before
 * being replicated between datastores. The consuming application is responsible
 * for finding active branches (also called conflicts), and marking the leaf
 * nodes of all branches but one deleted (thereby resolving the conflict).</p>
 *
 * <p>A {@code DocumentRevision} contains all the information for a single document
 * revision, including its ID and revision ID, along with the document's
 * content for this revision as a {@link DocumentBody} object. Clients will
 * typically set only the revision content rather than the metadata
 * explicitly.</p>
 */
public class DocumentRevision {

    // TODO can we make these final?
    /**
     * top level key: _id
     */
    protected String id;

    /**
     * top level key: _rev
     */
    protected String revision;

    /**
     * top level key: _deleted
     */
    protected boolean deleted;

    protected SimpleChangeNotifyingMap<String, Attachment> attachments =
            new SimpleChangeNotifyingMap<String, Attachment>(new HashMap<String, Attachment>());

    protected DocumentBody body;

    protected boolean bodyModified = false;

    public DocumentRevision() {
        // BasicDatastore#createDocumentFromRevision will assign an id
    }

    public DocumentRevision(String id) {
        this.id = id;
    }

    public DocumentRevision(String id, String revision) {
        this.id = id;
        this.revision = revision;
    }

    /**
     * @return the unique identifier of the document
     */
    public String getId() {
        return id;
    }

    /**
     * @return the revision ID of this document revision
     */
    public String getRevision() {
        return revision;
    }

    public boolean isDeleted() {
        return isDeleted();
    }

    /**
     * the rest of the document
     */
    public DocumentBody getBody() {
        return body;
    }

    public void setBody(DocumentBody body) {
        this.body = body;
        this.bodyModified = true;
    }

    /**
     * top level key: _attachments
     */
    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Attachment> attachments) {
        if (attachments != null) {
            this.attachments = new SimpleChangeNotifyingMap<String, Attachment>(attachments);
        } else {
            // user cleared the dict, we don't want our notifying map to try to forward to null
            this.attachments = null;
        }
        // user reset the whole attachments dict, this is a change
        this.bodyModified = true;
    }

    /*
     * Return a deep copy of body/attachments
     */
    public DocumentRevision copy() {
        // TODO
        return new DocumentRevision("empty", "empty");
    }

    /*
     * Helper used by sub-classes to convert between list and map representation of attachments
     */
    protected void setAttachmentsInternal(List<? extends Attachment> attachments)
    {
        if (attachments != null) {
            // this awkward looking way of doing things is to avoid marking the map as being modified
            HashMap<String, Attachment> m = new HashMap<String, Attachment>();
            for (Attachment att : attachments) {
                m.put(att.name, att);
            }
            this.attachments = new SimpleChangeNotifyingMap<String, Attachment>(m);
        }
    }

}
