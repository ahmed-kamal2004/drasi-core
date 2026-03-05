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

ISA-95 model can be used for topic design, and it is widely used by many organizations. (it is from 90s, and it was related more to ERP)
```
Enterprise/Site/Area/ProductionLine/WorkCell/Equipment/DataPoint
```
UNS (Unified namespace) uses topic structure as the backbone
```yaml
manufacturing/
  plantA/
    sensors/
      humidity/
        sensor001
        sensor002
    inventory/
      raw_materials/
        current_stock
      finished_goods/
        current_stock
    quality_control/
      inspection/
        results
```
So we can for example subscribe to `manufacturing/plantB/quality_control/testing/#` to receive data on all tests conducted in Plant B.









## 2) References
- https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/
- https://drasi.io/drasi-kubernetes/tutorials/curbside-pickup/
- https://www.machbase.com/en/post/how-to-make-sensor-data-send-directly-to-a-database-via-mqtt
- https://drasi.io/reference/query-language/
