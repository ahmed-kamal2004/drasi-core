# Design Document: Design notes for MQTT source.
<img width="1163" height="738" alt="Screenshot from 2026-03-05 15-39-12" src="https://github.com/user-attachments/assets/7a6cbec9-5097-444b-9330-2ad4b5bb5c4c" />

## Table of contents
- [Data Mapping](#1-data-mapping)
- [Data Format](#2-data-format)
- [MQTT v5 vs v3.1.1](#3-mqtt-v5-vs-v3.1.1)
- [Target brokers](#4-target-brokers)
- [References](#5-references)

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

One limitation I can think of, is that user needs to specify this hierarchy model as config, and then this model needs to be known by the middleware for mapping.

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
middleware will get the pattern, the topic name and the properties, then will map them to `SourceChange` data.

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

But the query becomes less readable, as labels like (`L0` - `L1` -,) will be used inside the query.

The middleware will need the topic name, properties only.

### Middleware
note: I think this part can be more optimized, after more going through the available options in the codebase and the communciation patterns between the middleware and the continous query.

the middleware should implement this trait `SourceMiddleWare`
```rust
#[async_trait]
pub trait SourceMiddleware: Send + Sync {
    async fn process(
        &self,
        source_change: SourceChange,
        element_index: &dyn ElementIndex,
    ) -> Result<Vec<SourceChange>, MiddlewareError>;
}
```
the function `process` accepts element index that has trait `ElementIndex`,

`ElementIndex` has a set of functions, the one we are interested in is

```rust
    async fn get_element(
        &self,
        element_ref: &ElementReference,
    ) -> Result<Option<Arc<Element>>, IndexError>;

    async fn set_element(
        &self,
        element: &Element,
        slot_affinity: &Vec<usize>,
    ) -> Result<(), IndexError>;
```

The functions `get_element`, `set_element` makes us able to check if a specific element exists in the index, before creating it.
so the proposed flow can be:
```pseudo
// Pseudocode
for x in set of nodes (except for the final sensor node, as it has different properties) and relations between those nodes:
  result = get_element(x) // element x (e.g building or room ) exists in the index
  if result is true: // the element exists
    do nothing.
  else
    set_element(x)
```


## 2) Data Format
// TODO
After doing a brief research, I believe `JSON` format should our primary target, then we can support more formats and the user can specify it as a config.

## 3) MQTT v5 vs v3.1.1
- MQTT v5 is backward compatible with v3.1.1
- (more related to the broker) Supports shared subscriptions (load balancing between subscribers over some algorithm e.g Round Robin).
- Sessions Expiry (How long should we maintain a session after disconnection), I don't think it is very effictive for client, as most of the time they are connected to few number of brokers
- Flow Control by broker and client, helpful to control how many messages can be sent without acknowledgement for QoS > 0 (I think this can be helpful in our case, can be added as a configuration).

Summary, I believe MQTT v5 is better for us for multiple reasons, the most imp reason is that it is backward compatible.

## 4) Target brokers
- Mosquitto
- HiveMQ

## 5) References
- https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/
- https://drasi.io/drasi-kubernetes/tutorials/curbside-pickup/
- https://www.machbase.com/en/post/how-to-make-sensor-data-send-directly-to-a-database-via-mqtt
- https://drasi.io/reference/query-language/
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901118 (not done yet)
- https://www.hivemq.com/blog/mqtt5-essentials-part6-user-properties/ (I think this is helpful for MQTT v5, not done yet)
- https://www.isa.org/standards-and-publications/isa-standards/isa-95-standard
