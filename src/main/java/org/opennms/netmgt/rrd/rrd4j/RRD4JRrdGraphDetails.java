/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2007-2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.rrd.rrd4j;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.rrd4j.graph.RrdGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opennms.netmgt.rrd.RrdException;
import org.opennms.netmgt.rrd.RrdGraphDetails;

/**
 * Container for details from a RRD4J RRD graph.  Stores the same details
 * as RrdGraphDetails, in addition to the RRD4J RrdGraph object itself and
 * the graph command String used to generate the graph.  We keep the graph
 * command string around so we can generate a detailed error if
 * getInputStream() is called, but no graph was produced.
 *
 * @author <a href="mailto:dj@opennms.org">DJ Gregor</a>
 * @version $Id: $
 */
public class RRD4JRrdGraphDetails implements RrdGraphDetails {
    private static final Logger LOG = LoggerFactory.getLogger(RRD4JRrdGraphDetails.class);
    
    private RrdGraph m_rrdGraph;
    private String m_graphCommand;

    /**
     * <p>Constructor for JRobinRrdGraphDetails.</p>
     *
     * @param rrdGraph a {@link org.jrobin.graph.RrdGraph} object.
     * @param graphCommand a {@link java.lang.String} object.
     */
    public RRD4JRrdGraphDetails(RrdGraph rrdGraph, String graphCommand) {
        m_rrdGraph = rrdGraph;
        m_graphCommand = graphCommand;
    }
    
    /**
     * <p>getRrdGraph</p>
     *
     * @return a {@link org.jrobin.graph.RrdGraph} object.
     */
    public RrdGraph getRrdGraph() {
        return m_rrdGraph;
    }
    
    /**
     * <p>getGraphCommand</p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getGraphCommand() {
        return m_graphCommand;
    }
    
    /**
     * <p>getInputStream</p>
     *
     * @return a {@link java.io.InputStream} object.
     * @throws org.opennms.netmgt.rrd.RrdException if any.
     */
    @Override
    public InputStream getInputStream() throws RrdException {
        assertGraphProduced();

        return new ByteArrayInputStream(m_rrdGraph.getRrdGraphInfo().getBytes());
    }

    /**
     * <p>getPrintLines</p>
     *
     * @return an array of {@link java.lang.String} objects.
     */
    @Override
    public String[] getPrintLines() {
        return m_rrdGraph.getRrdGraphInfo().getPrintLines();
    }

    /**
     * <p>getHeight</p>
     *
     * @return a int.
     * @throws org.opennms.netmgt.rrd.RrdException if any.
     */
    @Override
    public int getHeight() throws RrdException {
        assertGraphProduced();
        
        return m_rrdGraph.getRrdGraphInfo().getHeight();
    }

    /**
     * <p>getWidth</p>
     *
     * @return a int.
     * @throws org.opennms.netmgt.rrd.RrdException if any.
     */
    @Override
    public int getWidth() throws RrdException {
        assertGraphProduced();
        
        return m_rrdGraph.getRrdGraphInfo().getWidth();
    }

    private void assertGraphProduced() throws RrdException {
        if (m_rrdGraph.getRrdGraphInfo().getBytes() == null) {
            String message = "no graph was produced by RRD4J for command '" + getGraphCommand() + "'.  Does the command have any drawing commands (e.g.: LINE1, LINE2, LINE3, AREA, STACK, GPRINT)?";
            LOG.error(message);
            throw new RrdException(message);
        }
    }
}
