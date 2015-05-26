/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.encryption;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use this class to store a {@link KeyData} instance in the {@link SharedPreferences} associated
 * to an
 * identifier. To say in another way, it is possible to store multiple {@link KeyData}
 * instances as long as you use a {@link KeyStorage} with a different identifier for each
 * one.
 * <p/>
 * Each {@link KeyData} is bound to a specific identifier and all of them are grouped in
 * the keychain by service (service name defined with CDTENCRYPTION_KEYCHAINSTORAGE_SERVICE_VALUE).
 * This means that if you use the same identifier to store other data in the {@link
 * SharedPreferences} it will not
 * conflict with these values.
 *
 * @see KeyData
 */
public class KeyStorage {
    private static final Logger LOGGER = Logger.getLogger(KeyStorage.class.getCanonicalName());
    private static final String CDTENCRYPTION_KEYCHAINSTORAGE_SERVICE_VALUE =
            "com.cloudant.sync.CDTEncryptionKeychainStorage.keychain.service";

    private static final String PREF_NAME_DPK = "dpk"; //$NON-NLS-1$

    private static final String KEY_DPK = "dpk"; //$NON-NLS-1$
    private static final String KEY_ITERATIONS = "iterations"; //$NON-NLS-1$
    private static final String KEY_IV = "iv"; //$NON-NLS-1$
    private static final String KEY_SALT = "salt"; //$NON-NLS-1$
    private static final String KEY_VERSION = "version"; //$NON-NLS-1$

    private String service;
    private String account;
    private SharedPreferences prefs;

    /**
     * Create a storage with an identifier. A {@link KeyData} saved to the {@link
     * SharedPreferences} using this storage will be bound to the identifier specified here.
     *
     * @param context    The applications {@link Context}
     * @param identifier A string
     */
    public KeyStorage(Context context, String identifier) {
        if (identifier != null && context != null) {
            this.service = CDTENCRYPTION_KEYCHAINSTORAGE_SERVICE_VALUE;
            this.account = identifier;
            this.prefs = context.getSharedPreferences(this.service, Context.MODE_PRIVATE);
        } else {
            throw new IllegalArgumentException("All parameters are required");
        }
    }

    /**
     A {@link KeyData} previously saved with this storage (or other storage created before
     with the same identifier).

     @return A {@link KeyData} saved before or null
     */
    public KeyData getEncryptionKeyData() {
        KeyData encryptionData = null;

        String savedValue = this.prefs.getString(buildKey(), null);
        if (savedValue != null) {
            JSONObject savedObject = null;
            try {
                savedObject = convertStringToJSON(savedValue);
                encryptionData = new KeyData(DKPEncryptionUtil.hexStringToByteArray(savedObject
                        .getString(KEY_DPK)), DKPEncryptionUtil.hexStringToByteArray
                        (savedObject.getString(KEY_SALT)), DKPEncryptionUtil
                        .hexStringToByteArray(savedObject.getString(KEY_IV)), savedObject.getInt
                        (KEY_ITERATIONS), savedObject.getString(KEY_VERSION));
            } catch (JSONException e) {
                LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                return null;
            }
        }
        return encryptionData;
    }

    /*
     Save to the {@link SharedPreferences} a {@link KeyData}.

     Notice that if there is already data in the {@link SharedPreferences} bound to the same identifier used to create
     this storage, the operation will fail.

     @param data {@link KeyData} to save to the {@link SharedPreferences}

     @return true (success) or false (fail)
     */
    public boolean saveEncryptionKeyData(KeyData data) {
        JSONObject objectToSave = new JSONObject();
        try {
            objectToSave.put(KEY_DPK, DKPEncryptionUtil.byteArrayToHexString(data
                    .getEncryptedDPK()));
            objectToSave.put(KEY_IV, DKPEncryptionUtil.byteArrayToHexString(data.getIv()));
            objectToSave.put(KEY_SALT, DKPEncryptionUtil.byteArrayToHexString(data.getSalt()));
            objectToSave.put(KEY_ITERATIONS, data.getIterations());
            objectToSave.put(KEY_VERSION, data.getVersion());

            String valueToSave = objectToSave.toString();

            SharedPreferences.Editor editor = this.prefs.edit();
            editor.putString(buildKey(), valueToSave);
            editor.commit();

        } catch (JSONException e) {
            LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
            return false;
        }

        return true;
    }

    /**
     Remove from the {@link SharedPreferences} a {@link KeyData} associated to the same identifier used to
     create this storage.

     It will succeed if the data is deleted or if there is no data at all.

     @return true (success) or false (fail)
     */
    public boolean clearEncryptionKeyData() {
        try {
            SharedPreferences.Editor editor = this.prefs.edit();
            editor.remove(buildKey());
            editor.commit();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
            return false;
        }
        return true;
    }

    /**
     Look for data saved in the {@link SharedPreferences} with the same identifier used to create this storage instance.

     @return true (data found) or false (data not found)
     */
    public boolean encryptionKeyDataExists() {
        return this.prefs.contains(buildKey());
    }

    private String buildKey() {
        return PREF_NAME_DPK + "-" + this.account;
    }

    private JSONObject convertStringToJSON(String jsonString) throws JSONException {
        int beginIndex = jsonString.indexOf("{");
        int endIndex = jsonString.lastIndexOf("}");

        if (beginIndex == -1 || endIndex == -1 || beginIndex > endIndex + 1) {
            String message = "Input string does not contain brackets, or input string is invalid." +
                    " The string is: " + jsonString;
            //logger.debug (message);
            throw new JSONException(message);
        }

        String secureJSONString = jsonString.substring(beginIndex, endIndex + 1);
        JSONObject jsonObject = new JSONObject(secureJSONString);
        return jsonObject;
    }
}