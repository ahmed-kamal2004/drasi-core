use crate::config::MQTTConnectionConfig;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS};

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
}
