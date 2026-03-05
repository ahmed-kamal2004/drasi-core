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
UNS (Unified namespace) uses topic structure as the backbone.
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

UNS ans ISA-95 are complementary.

### How we can see the hierarchy ?
In MQTT Source, we are interested in building the hierarchy to allow users write effective queries that exactly match what they need.

an example for a simple use case, which just retrieving the available readings
```yaml
MATCH (b:Building {id: "building-1"})
      -[:HAS_FLOOR]->(f:Floor {id: "floor-2"})
      -[:HAS_SENSOR]->(s:Sensor {id: "sensor-7"})
RETURN b.id AS building, f.id AS floor, s.id AS sensor, s.value
```
more complex one, it checks if the temperature is more than 25 C for more than 15 secs. 
```yaml
MATCH (b:Building)-[:HAS_FLOOR]->(f:Floor)-[:HAS_ROOM]->(r:Room)-[:HAS_SENSOR]->(s:Sensor)
WHERE s.type = 'temperature'

WITH
  b, f, r, s,
  drasi.changeDateTime(s) AS temperatureChangeTime

WHERE
  temperatureChangeTime != datetime({epochMillis: 0}) AND
  drasi.trueFor(
    s.value > 25,
    duration({ seconds: 15 })
  )

RETURN
  b.id AS buildingId,
  f.id AS floorId,
  r.id AS roomId,
  s.id AS sensorId,
  s.value AS temperature,
  temperatureChangeTime AS fireRiskDetectedSince
```

So in order to map the topic hierarchy to the internal schema in the continouos query model, I can think of multiple available options.
##### Using Hierarchy
In this approach, the hierarchy model can pre specified by the user as config

for example, if the user specified this hierarchy model
```
Building/Floor/Sensor
```
then a topic name like 
```
campus/the-first-floor/temperature
```
can be mapped with labels like 
```
campus -> Building
the-first-floor -> Floor
temperature -> Sensor
```

This approach gives the users the freedom to use any names for the topic entities, as long as they are following the structure they specified

One limitation I can think of, is that user needs to specify that hierarchy model, and then this model needs to be known by the middleware for mapping.

##### Using seperator
In this approach, a seperator or regex can pre specified by the user as config

for example, if the user specified this seperator
```
- (dash)
```
then a topic name like 
```
building-1/floor-3/sensor-2
```
can be mapped with labels like 
```
building-1 -> building
floor-3 -> floor
sensor-2 -> sensor
```

This approach limits the naming for the topic entities, it needs to follow pattern to be correctly handled.

##### Level mapping
In this approach, nothing needs to be pre-specified by the user as config

for a topic name like 
```
building-1/floor-3/sensor-2
```
can be mapped with labels like 
```
building-1 -> L0
floor-3 -> L1
sensor-2 -> L2
```

But the query less readable, as labels like (`L0` - `L1` -,) will be used inside the query.

### Middleware
// TODO

## 2) Data Format
// TODO

## 3) MQTT v5 vs v3.1.1

// TODO


## 2) References
- https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/
- https://drasi.io/drasi-kubernetes/tutorials/curbside-pickup/
- https://www.machbase.com/en/post/how-to-make-sensor-data-send-directly-to-a-database-via-mqtt
- https://drasi.io/reference/query-language/
