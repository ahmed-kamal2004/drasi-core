# Design Document: Design notes for MQTT source.
<img width="1163" height="738" alt="Screenshot from 2026-03-05 15-39-12" src="https://github.com/user-attachments/assets/7a6cbec9-5097-444b-9330-2ad4b5bb5c4c" />

## 1) Data Mapping
The common cases for payload sent from the sensor to the broker can be in the following shape
```json
{
  "timestamp": "2026-03-05T10:15:30Z",
  "type": "temperature",
  "value": 4.2,
  "unit": "C"
}
```

or 

```json
{
  "temperature": "4.2"
}
```

the topic which the payload is sent over could be for example `building-1/floor-1/room-2/sensor-1` which should be following a pre-specified topic structure and hierarchy.

### Topic hierarchy
Designing a topic hierarchy helps organizations to easily and efficiently use topics, allowing controlling access to the multi-level topic hierarchy.

ISA-95 model for topic design is widely used by many organizations.
```
Enterprise/Site/Area/ProductionLine/WorkCell/Equipment/DataPoint
```







## 2) References
- https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/
- https://drasi.io/drasi-kubernetes/tutorials/curbside-pickup/
- https://www.machbase.com/en/post/how-to-make-sensor-data-send-directly-to-a-database-via-mqtt
- https://drasi.io/reference/query-language/
