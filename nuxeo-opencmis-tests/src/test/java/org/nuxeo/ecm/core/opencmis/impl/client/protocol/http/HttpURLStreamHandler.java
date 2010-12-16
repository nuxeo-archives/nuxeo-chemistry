/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Stephane Lacoin (aka matic)
 */
package org.nuxeo.ecm.core.opencmis.impl.client.protocol.http;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class HttpURLStreamHandler extends URLStreamHandler {

    final HttpURLClientProvider clientProvider;

    public HttpURLStreamHandler(HttpURLClientProvider provider) {
        this.clientProvider = provider;
    }

    @Override
    protected URLConnection openConnection(URL url) throws IOException {
        URLConnection connection = new HttpURLConnection(clientProvider, url);
        connection.connect();
        return connection;
    }

}
