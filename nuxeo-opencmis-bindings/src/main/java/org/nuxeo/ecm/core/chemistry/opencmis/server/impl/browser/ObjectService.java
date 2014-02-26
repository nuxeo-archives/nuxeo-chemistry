/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.nuxeo.ecm.core.chemistry.opencmis.server.impl.browser;

import static org.apache.chemistry.opencmis.commons.impl.Constants.MEDIATYPE_OCTETSTREAM;
import static org.apache.chemistry.opencmis.commons.impl.Constants.PARAM_STREAM_ID;
import static org.apache.chemistry.opencmis.server.impl.browser.BrowserBindingUtils.CONTEXT_OBJECT_ID;
import static org.apache.chemistry.opencmis.server.impl.browser.BrowserBindingUtils.setStatus;
import static org.nuxeo.ecm.core.chemistry.opencmis.server.shared.HttpUtils.getStringParameter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.impl.MimeHelper;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.commons.server.CmisService;
import org.nuxeo.ecm.core.chemistry.opencmis.server.shared.HttpUtils;

/**
 * Object Service operations.
 */
public final class ObjectService {

    private static final int BUFFER_SIZE = 64 * 1024;

    private ObjectService() {
    }

    /**
     * getContentStream.
     */
    public static void getContentStream(CallContext context, CmisService service, String repositoryId,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        // get parameters
        String objectId = (String) context.get(CONTEXT_OBJECT_ID);
        String streamId = getStringParameter(request, PARAM_STREAM_ID);

        BigInteger offset = context.getOffset();
        BigInteger length = context.getLength();

        // execute
        ContentStream content = service.getContentStream(repositoryId, objectId, streamId, offset, length, null);

        if (content == null || content.getStream() == null) {
            throw new CmisRuntimeException("Content stream is null!");
        }

        // set HTTP headers, if requested by the server implementation
        if (HttpUtils.setContentStreamHeaders(content, request, response)) {
            return;
        }

        String contentType = content.getMimeType();
        if (contentType == null) {
            contentType = MEDIATYPE_OCTETSTREAM;
        }

        // set headers
        if (offset == null && length == null) {
            response.setStatus(HttpServletResponse.SC_OK);
        } else {
            setStatus(request, response, HttpServletResponse.SC_PARTIAL_CONTENT);
        }
        response.setContentType(contentType);
        if (content.getFileName() != null) {
            response.setHeader(MimeHelper.CONTENT_DISPOSITION,
                    MimeHelper.encodeContentDisposition(MimeHelper.DISPOSITION_INLINE, content.getFileName()));
        }

        // send content
        InputStream in = new BufferedInputStream(content.getStream(), BUFFER_SIZE);
        OutputStream out = new BufferedOutputStream(response.getOutputStream());

        byte[] buffer = new byte[BUFFER_SIZE];
        int b;
        while ((b = in.read(buffer)) > -1) {
            out.write(buffer, 0, b);
        }

        in.close();
        out.flush();
    }

}
