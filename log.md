Pour accéder à la mesure détaillée de puissance active pour le point d’usage 30001610071843

Client:

```xml
<iq xml:lang="en" to="…" from="…" type="set" id="f897d">
  <command xmlns="http://jabber.org/protocol/commands" node="get_history" action="execute">
    <x xmlns="jabber:x:data" type="submit">
      <field var="identifier" type="text-single">
        <value>urn:dev:prm:30001610071843_consumption/power/active/raw</value>
      </field>
      <field var="start_time" type="text-single">
        <value>2020-03-01T00:00:00+01:00</value>
      </field>
      <field var="end_time" type="text-single">
        <value>2020-03-08T00:00:00+01:00</value>
      </field>
    </x>
  </command>
</iq>
```

Serveur

```xml
<iq xml:lang="en" to="…" type="result" id="f897d">
  <command xmlns="http://jabber.org/protocol/commands" node="get_history" status="completed">
    <x xmlns="jabber:x:data" type="result">
      <title>Get history</title>
      <field var="result" type="fixed" label="Get urn:dev:prm:30001610071843_consumption/power/active/raw">
        <value>Success</value>
      </field>
    </x>
    <quoalise xmlns="urn:quoalise:0">
      <data>
        <meta>
          <device type="electricity-meter">
            <identifier authority="enedis" type="prm" value="30001610071843"/>
          </device>
          <measurement name="active-power" direction="consumption" quantity="power" type="electrical" unit="W" aggregation="mean" sampling-interval="PT10M"/>
        </meta>
        <sensml xmlns="urn:ietf:params:xml:ns:senml">
          <senml bn="urn:dev:prm:30001610071843_consumption/power/active/raw" bt="1583016600" t="0" v="100" bu="W"/>
          <senml t="600" v="101"/>
          <!-- … -->
          <senml t="604200" v="243"/>
        </sensml>
      </data>
    </quoalise>
  </command>
</iq>
```