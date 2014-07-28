/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.qpid.router010;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.message.Message;
import org.qpid.router010.NetworkConnection.ByteReceiver;

public class RouterManager implements NetworkConnection.ByteReceiver
{
    private final NetworkConnection _network;

    private Connection _connection = Proton.connection();

    private Collector _collector = Proton.collector();

    private Transport _transport = Proton.transport();

    private Session _ssn;

    private Sender _outgoingLink;

    private int _maxCredits = 5000;

    private final Map<String, Router> _routers = new HashMap<String, Router>();

    private final AtomicLong _deliveryTag = new AtomicLong(0);

    private String routerAddr = "010_ROUTER";
    
    RouterManager(String host, int port)
    {
        _connection.collect(_collector);
        _connection.setContainer(UUID.randomUUID().toString());
        _connection.setHostname(host);
        _transport.bind(_connection);
        Sasl sasl = _transport.sasl();
        sasl.client();
        sasl.setMechanisms(new String[] { "ANONYMOUS" });
        _connection.open();
        _network = new NetworkConnection(host, port, this);
        write();
    }

    void write()
    {
        while (_transport.pending() > 0)
        {
            ByteBuffer data = _transport.getOutputBuffer();
            try
            {
                _network.send(data);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Exception writing to socket", e);
            }
            _transport.outputConsumed();
        }
    }

    @Override
    public void received(ByteBuffer data)
    {
        while (data.hasRemaining())
        {
            ByteBuffer buf = _transport.getInputBuffer();
            int maxAllowed = Math.min(data.remaining(), buf.remaining());
            ByteBuffer temp = data.duplicate();
            temp.limit(data.position() + maxAllowed);
            buf.put(temp);
            _transport.processInput();
            data.position(data.position() + maxAllowed);
        }
        processEvents();
        write();
    }

    @Override
    public void exception(Exception e)
    {
        // TODO Auto-generated method stub
        e.printStackTrace();
    }

    void processEvents()
    {
        while (true)
        {
            Event event = _collector.peek();
            if (event == null)
                break;

            switch (event.getType())
            {
            case CONNECTION_OPEN:
                setUp();
                break;
            case LINK_REMOTE_CLOSE:
                System.out.println("Link closed : " + event.getLink().getName() + " , address : "
                        + event.getLink().getSource().getAddress());
                break;
            case LINK_FLOW:
                // onFlow(event.getLink());
                break;
            case DELIVERY:
                onDelivery(event.getDelivery());
                break;
            default:
                break;
            }
            _collector.pop();
        }
    }

    void onDelivery(Delivery d)
    {
        if (d.getLink() instanceof Receiver)
        {
            if (d.isPartial())
            {
                return;
            }

            Receiver receiver = (Receiver) d.getLink();
            byte[] bytes = new byte[d.pending()];
            int read = receiver.recv(bytes, 0, bytes.length);
            Message msg = Proton.message();
            msg.decode(bytes, 0, read);

            onMessage(d, msg);
        }
        else
        {
            if (d.remotelySettled())
            {
                Delivery incoming = (Delivery) d.getContext();
                if (d.getRemoteState() != null)
                {
                    d.disposition(d.getRemoteState());
                    incoming.disposition(d.getRemoteState());
                }
                incoming.settle();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    void onMessage(Delivery d, Message msg)
    {
        Map props = msg.getApplicationProperties() == null ? null : msg.getApplicationProperties().getValue();
        if (props != null && props.containsKey("MGT_MSG"))
        {
            handleMgtMessage(msg);
            d.settle();
        }
        else
        {
            route(d, msg);
        }
        if (d.getLink().getCredit() < _maxCredits / 2)
        {
            ((Receiver) d.getLink()).flow(_maxCredits - d.getLink().getCredit());
        }
    }

    void handleMgtMessage(Message msg)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = msg.getApplicationProperties().getValue();
        String opCode = (String) map.get("OP_CODE");
        String address = (String) map.get("ADDR");
        String pattern = (String) map.get("PATTERN");
        String destination = (String) map.get("DEST");

        if ("ADD_ROUTE".equals(opCode))
        {
            if (_routers.containsKey(address))
            {
                _routers.get(address).addRoute(pattern, destination);
            }
            else
            {
                Router router = new Router(address, createLink(address));
                router.addRoute(pattern, destination);
                _routers.put(address, router);
            }
        }
        else if ("REMOVE_ROUTE".equals(opCode))
        {
            if (_routers.containsKey(address))
            {
                Router router = _routers.get(address);
                router.removeRoute(pattern);
                if (router.size() == 0)
                {
                    removeLink(router.getLink());
                    _routers.remove(address);
                }
            }
        }
        else
        {
            //
        }
    }

    void route(Delivery d, Message msg)
    {
        String address = msg.getAddress();
        String routingKey = msg.getSubject() != null ? msg.getSubject() : (String) msg.getApplicationProperties()
                .getValue().get("x-amqp-0-10.routing-key");

        if (routingKey == null)
        {
            System.out.println("Incomplete routing information, routing to default address");
            return;
        }

        msg.getMessageAnnotations().getValue().remove("x-opt-qd.trace");

        Router router = _routers.get(address);
        List<String> destinations = router.route(routingKey);
        
        if (destinations.size() == 0)
        {
            System.out.println("no matching address, routing to DLQ");
        }
        
        for (String destination : destinations)
        {
            msg.setAddress(destination);
            send(d.isSettled() ? null : d, msg);
        }
    }

    // Sets up a link with the router network for receiving messages
    Link createLink(String address)
    {
        Receiver rcv = _ssn.receiver(address);
        Source source = new Source();
        source.setAddress(address);
        rcv.setSource(source);
        rcv.open();
        rcv.flow(_maxCredits);
        return rcv;
    }

    void removeLink(Link link)
    {
        link.close();
    }

    void setUp()
    {
        _ssn = _connection.session();
        _ssn.open();
        Receiver rcv = _ssn.receiver("010_ROUTER");
        Source source = new Source();
        source.setAddress(routerAddr);
        rcv.setSource(source);
        rcv.open();
        rcv.flow(_maxCredits);

        _outgoingLink = _ssn.sender("RouterManager.OUTLINK");
        _outgoingLink.open();
    }

    void send(Delivery incomming, Message msg)
    {
        byte[] tag = getNextDeliveryTag();
        Delivery outgoing = _outgoingLink.delivery(tag);

        if (incomming == null)
        {
            outgoing.settle();
        }
        else
        {
            outgoing.setContext(incomming);
        }
        byte[] buffer = new byte[1024];
        int encoded = msg.encode(buffer, 0, buffer.length);
        _outgoingLink.send(buffer, 0, encoded);
        _outgoingLink.advance();
    }

    byte[] getNextDeliveryTag()
    {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(_deliveryTag.incrementAndGet());
        return buffer.array();
    }

    class Router
    {
        final String _address;

        final Link _link;

        final Map<Pattern, String> _routes = new HashMap<Pattern, String>();

        Router(String address, Link link)
        {
            _address = address;
            _link = link;
        }

        Link getLink()
        {
            return _link;
        }

        void addRoute(String str, String destination)
        {
            Pattern pattern = Pattern.compile(str);
            _routes.put(pattern, destination);
            System.out.println("Added route " + str + " for dest " + destination);
        }

        void removeRoute(String pattern)
        {
            _routes.remove(Pattern.compile(pattern));
        }

        int size()
        {
            return _routes.size();
        }

        List<String> route(String key)
        {
            List<String> dests = new LinkedList<String>();
            for (Pattern pattern : _routes.keySet())
            {
                if (pattern.matcher(key).matches())
                {
                    dests.add(_routes.get(pattern));
                }
            }
            return dests;
        }
    }

    class Route
    {
        Pattern _pattern;

        Route(Pattern pattern)
        {
            _pattern = pattern;
        }

        boolean matches(String str)
        {
            return _pattern.matcher(str).matches();
        }

        @Override
        public int hashCode()
        {
            return 256 + 23 * _pattern.pattern().hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof Route && ((Route) o)._pattern.pattern().equals(_pattern.pattern());
        }
    }
}