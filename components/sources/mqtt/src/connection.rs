use crate::{config::MQTTConnectionConfig, model::{MQTTSourceChange, convert_mqtt_to_source_change, map_json_to_mqtt_source_change}};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS};
use crate::SourceChangeEvent;
use log::{debug, error, info, trace, warn};
use anyhow::{Result};
pub struct MQTTConnectionWrapper {
    client: Box<AsyncClient>,
    eventloop: Box<EventLoop>,
    options: Box<MqttOptions>,
    qos: QoS,
    channel_capacity: usize,
    timeout_ms: u64,
    topic: String,
}

impl MQTTConnectionWrapper {
    pub fn new(config: MQTTConnectionConfig) -> Self {
        let (client, eventloop) = AsyncClient::new(config.options.clone(), config.channel_capacity);
        Self {
            client: Box::new(client),
            eventloop: Box::new(eventloop),
            options: Box::new(config.options),
            qos: config.qos,
            channel_capacity: config.channel_capacity,
            timeout_ms: config.timeout_ms,
            topic: config.topic,
        }
    }

    pub fn client(&self) -> &AsyncClient {
        &self.client
    }

    pub fn eventloop(&self) -> &EventLoop {
        &self.eventloop
    }

    pub fn options(&self) -> &MqttOptions {
        &self.options
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        self.client.subscribe(self.topic.clone(), self.qos).await?;
        loop {
            let event = self.eventloop.poll().await?;
            match event {
                Event::Incoming(incoming) => {
                    println!("MQTT Incoming: {:?}", incoming);

                    match incoming {
                        Incoming::Publish(publish) => {
                            println!(
                                "Received message on topic {}: {:?}",
                                publish.topic, publish.payload
                            );
                            let event = map_json_to_mqtt_source_change(&String::from_utf8_lossy(&publish.payload))?;
                            let source_id = "mqtt-source";
                            Self::process_events(source_id, event).await?;
                        }
                        _ => {}
                    }
                }
                Event::Outgoing(outgoing) => {
                    println!("MQTT Outgoing: {:?}", outgoing);
                }
            }
        }
    }


    async fn process_events(
        source_id: &str,
        event: MQTTSourceChange,
    ) -> Result<()> {
        trace!("[{}] Processing MQTT event", source_id);

        match convert_mqtt_to_source_change(&event, source_id) {
                Ok(source_change) => {
                    let change_event = SourceChangeEvent {
                        source_id: source_id.to_string(),
                        change: source_change,
                        timestamp: chrono::Utc::now(),
                    };

                    println!(
                        "[{}] Converted MQTT event to SourceChangeEvent: {:?}",
                        source_id, change_event
                    );
                }
                Err(e) => {
                    error!(
                        "[{}] Failed to convert MQTT event to SourceChangeEvent: {:?}",
                        source_id, e
                    );
                }
        }
        Ok(())
    }
}