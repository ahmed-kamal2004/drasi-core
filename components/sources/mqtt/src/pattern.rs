use drasi_core::evaluation::variable_value::de;
use matchit::{Match, Params, Router};
use serde_yaml::mapping;

use crate::schema::{convert_mqtt_to_source_change, MqttSourceChange};
use crate::utils::MqttPacket;
use crate::{
    config::{MappingMode, MappingNode, MappingRelation, TopicMapping},
    MQTTSource, MqttElement,
};
use drasi_core::models::{ElementMetadata, ElementReference};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PatternMatcher {
    pub router: Router<TopicMapping>,
}

impl PatternMatcher {
    pub fn new(topic_mappings: &Vec<TopicMapping>) -> Self {
        let mut router = Router::new();
        for mapping in topic_mappings {
            router.insert(&mapping.pattern, mapping.clone()).unwrap();
        }
        Self { router }
    }

    pub fn generate_schema(&self, packet: &MqttPacket) -> anyhow::Result<Vec<MqttSourceChange>> {
        match self.router.at(&packet.topic) {
            Ok(matched) => {
                let mut result = Vec::new();
                let mapping = matched.value;
                let params = matched.params;

                match Self::create_entity(mapping, &params, packet) {
                    Ok(change) => result.push(change),
                    Err(e) => {
                        log::error!(
                            "Error processing matched pattern for topic {}: {}",
                            packet.topic,
                            e
                        );
                        return Err(e);
                    }
                }

                let timestamp = packet.timestamp;
                let mut hierarchy = Self::create_hierarchy(mapping, &params, timestamp);
                result.append(&mut hierarchy);

                Ok(result)
            }
            Err(_) => {
                // no pattern is matched
                Err(anyhow::anyhow!(
                    "No pattern matched for topic: {}",
                    packet.topic
                ))
            }
        }
    }

    fn create_entity(
        mapping: &TopicMapping,
        params: &Params,
        packet: &MqttPacket,
    ) -> anyhow::Result<MqttSourceChange> {
        // mapping is validated from an initial step, so we can safely unwrap the field_name for PayloadAsField mode

        // payload parsing based on the mapping mode
        let mut properties = match &mapping.properties.mode {
            MappingMode::PayloadAsField => {
                let field_name =
                    Self::map_template(mapping.properties.field_name.as_ref().unwrap(), params);
                let mut props = serde_json::Map::new();
                let val = serde_json::from_slice(&packet.payload).unwrap_or_else(|_| {
                    serde_json::Value::String(String::from_utf8_lossy(&packet.payload).into_owned())
                });
                props.insert(field_name, val);
                props
            }
            MappingMode::PayloadSpread => {
                match serde_json::from_slice::<serde_json::Value>(&packet.payload) {
                    Ok(serde_json::Value::Object(map)) => map,
                    Ok(_) => {
                        log::error!("Expected JSON object in payload for PayloadSpread mode, got different type. Defaulting to empty properties.");
                        serde_json::Map::new()
                    }
                    Err(e) => {
                        log::error!("Failed to parse payload as JSON for PayloadSpread mode: {}. Defaulting to empty properties.", e);
                        serde_json::Map::new()
                    }
                }
            }
        };

        // injection of topic variables as properties
        for inject in &mapping.properties.inject {
            for (key, template) in inject {
                let value = Self::map_template(template, params);
                properties.insert(key.clone(), serde_json::Value::String(value));
            }
        }

        // entity main label and id mapping
        let entity_label = Self::map_template(&mapping.entity.label, params);
        let entity_id = Self::map_template(&mapping.entity.id, params);

        let element = MqttElement::Node {
            id: entity_id.clone(),
            labels: vec![entity_label.clone()],
            properties,
        };
        Ok(MqttSourceChange::Update {
            element,
            timestamp: Some(packet.timestamp),
        })
    }

    fn create_hierarchy(
        mapping: &TopicMapping,
        params: &Params,
        timestamp: u64,
    ) -> Vec<MqttSourceChange> {
        let mut hierarchy = Vec::new();
        hierarchy.append(&mut Self::create_nodes(
            mapping.nodes.as_ref(),
            params,
            timestamp,
        ));
        hierarchy.append(&mut Self::create_relations(
            mapping.relations.as_ref(),
            params,
            timestamp,
        ));
        hierarchy
    }

    fn create_nodes(
        nodes: &Vec<MappingNode>,
        params: &Params,
        timestamp: u64,
    ) -> Vec<MqttSourceChange> {
        let mut result = Vec::new();
        for node in nodes {
            let node_id = Self::map_template(&node.id, params);
            let element = MqttElement::Node {
                id: node_id.clone(),
                labels: vec![node.label.clone()],
                properties: serde_json::Map::new(),
            };
            result.push(MqttSourceChange::Update {
                element,
                timestamp: Some(timestamp),
            });
        }
        return result;
    }

    fn create_relations(
        relations: &Vec<MappingRelation>,
        params: &Params,
        timestamp: u64,
    ) -> Vec<MqttSourceChange> {
        let mut result = Vec::new();
        for relation in relations {
            let relation_id = Self::map_template(&relation.id, params);
            let element = MqttElement::Relation {
                id: relation_id.clone(),
                labels: vec![relation.label.clone()],
                from: relation.from.clone(),
                to: relation.to.clone(),
                properties: serde_json::Map::new(),
            };
            result.push(MqttSourceChange::Update {
                element,
                timestamp: Some(timestamp),
            });
        }
        return result;
    }

    fn map_template(template: &str, params: &Params) -> String {
        // needs unit_test
        let mut id = template.to_string();
        for (key, value) in params.iter() {
            id = id.replace(&format!("{{{}}}", key), value);
        }
        id
    }
}
