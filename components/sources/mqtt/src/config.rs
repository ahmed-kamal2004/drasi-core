use crate::QualityOfService;
use rumqttc::{MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MQTTSourceConfig {
    /// MQTT broker host address
    #[serde(default = "default_host")]
    pub host: String,

    /// MQTT broker port
    #[serde(default = "default_port")]
    pub port: u16,

    /// MQTT topic to subscribe to
    #[serde(default = "default_topic")]
    pub topic: String,

    /// Optional username for MQTT authentication
    pub username: Option<String>,

    /// Optional password for MQTT authentication
    pub password: Option<String>,

    /// Quality of Service level for MQTT messages
    #[serde(default = "default_qos")]
    pub qos: QualityOfService,

    /// Capacity of the internal channel buffer for incoming messages
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Optional timeout in milliseconds for MQTT operations
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

pub fn default_host() -> String {
    "localhost".to_string()
}

pub fn default_port() -> u16 {
    1883
}

pub fn default_topic() -> String {
    "topic".to_string()
}

pub fn default_qos() -> QualityOfService {
    QualityOfService::ExactlyOnce
}

pub fn default_channel_capacity() -> usize {
    100
}

pub fn default_timeout_ms() -> u64 {
    5000
}

impl MQTTSourceConfig {
    pub fn new() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            topic: default_topic(),
            username: None,
            password: None,
            qos: default_qos(),
            channel_capacity: default_channel_capacity(),
            timeout_ms: default_timeout_ms(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.host.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "Validation error: host cannot be empty. \
                 Please specify a valid host address for the MQTT broker."
            ));
        }
        if self.port == 0 {
            return Err(anyhow::anyhow!(
                "Validation error: port cannot be 0. \
                 Please specify a valid port number for the MQTT broker (1-65535)."
            ));
        }
        if self.topic.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "Validation error: topic cannot be empty. \
                 Please specify a valid topic for the MQTT broker."
            ));
        }
        if self.timeout_ms == 0 {
            return Err(anyhow::anyhow!(
                "Validation error: timeout_ms cannot be 0. \
                 Please specify a positive timeout value in milliseconds."
            ));
        }
        Ok(())
    }

    pub fn to_mqtt_connection_config(&self, id: impl Into<String>) -> MQTTConnectionConfig {
        let mut options = MqttOptions::new(id.into(), self.host.clone(), self.port);
        if let Some(username) = &self.username {
            options.set_credentials(username, self.password.as_deref().unwrap_or(""));
        }
        MQTTConnectionConfig {
            options,
            qos: match self.qos {
                QualityOfService::AtMostOnce => QoS::AtMostOnce,
                QualityOfService::AtLeastOnce => QoS::AtLeastOnce,
                QualityOfService::ExactlyOnce => QoS::ExactlyOnce,
            },
            channel_capacity: self.channel_capacity,
            timeout_ms: self.timeout_ms,
            topic: self.topic.clone(),
        }
    }
}

pub struct MQTTConnectionConfig {
    pub options: MqttOptions,
    pub qos: QoS,
    pub channel_capacity: usize,
    pub timeout_ms: u64,
    pub topic: String,
}
