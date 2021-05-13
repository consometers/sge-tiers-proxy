#!/usr/bin/env python3

import slixmpp
import xml
from slixmpp.xmlstream import tostring
from xml.etree.ElementTree import fromstring
from xml.sax.saxutils import escape
import asyncio
import utils

class SgeProxyClient:

    def __init__(self, xmpp_client, proxy_full_jid):
        self.xmpp_client = xmpp_client
        self.proxy_full_jid = proxy_full_jid

    # Identifier: urn:dev:prm:30001610071843_consumption/active_power/raw
    async def get_measurement(self, identifier, start_date, end_date):
        iq = self.xmpp_client.Iq()
        iq['type'] = 'set'
        iq['to'] = self.proxy_full_jid

        iq.append(fromstring(f"""
            <command xmlns="http://jabber.org/protocol/commands" node="get_measurements" action="execute">
              <x xmlns="jabber:x:data" type="submit">
                <field var="identifier" type="text-single">
                  <value>{escape(identifier)}</value>
                </field>
                <field var="start_date" type="text-single">
                  <value>{escape(utils.format_iso_date(start_date))}</value>
                </field>
                <field var="end_date" type="text-single">
                  <value>{escape(utils.format_iso_date(end_date))}</value>
                </field>
              </x>
            </command>
        """))

        response = await iq.send()

        command = response.xml.find('.//{http://jabber.org/protocol/commands}command')
        if command.attrib['status'] == "completed":
            return command.find('.//{jabber:x:data}value').text
        else:
            raise RuntimeError("Unexpected iq response: " + tostring(response.xml))

    @classmethod
    async def connect(cls, client_jid, client_password, proxy_full_jid):
        xmpp_client = slixmpp.ClientXMPP(client_jid, client_password)
        xmpp_client.connect()
        await xmpp_client.wait_until('session_start')
        return cls(xmpp_client, proxy_full_jid)

    def disconnect(self):
        self.xmpp_client.disconnect()
        # Avoids « Task was destroyed but it is pending! » when closing the event loop,
        # might not be needed in future versions
        self.xmpp_client._run_out_filters.cancel()

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main(local_jid, password, args.prm, args.start_date, args.end_date, args.direction))