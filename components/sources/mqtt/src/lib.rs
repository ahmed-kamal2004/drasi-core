pub mod auth;
pub mod config;
pub mod connection;
pub mod model;

use config::MQTTSourceConfig;
use drasi_core::evaluation::functions::async_trait;
use drasi_lib::config::SourceSubscriptionSettings;
use drasi_lib::queries::base;
use drasi_lib::sources::base::{SourceBase, SourceBaseParams};
use drasi_lib::ComponentStatus;
use drasi_lib::Source;
use std::collections::HashMap;

use anyhow::Result;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tower_http::cors::{Any, CorsLayer};

use crate::config::{default_channel_capacity, default_qos, default_timeout_ms};
use crate::model::QualityOfService;
use drasi_lib::channels::{ComponentType, *};
use drasi_lib::SourceRuntimeContext;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Packet, QoS};

pub struct MQTTSource {
    base: SourceBase,
    config: MQTTSourceConfig,
}

impl MQTTSource {
    pub fn new(id: impl Into<String>, config: MQTTSourceConfig) -> Result<Self> {
        let base_params = SourceBaseParams::new(id);
        Ok(Self {
            base: SourceBase::new(base_params)?,
            config,
        })
    }

    pub fn with_dispatch(
        id: impl Into<String>,
        config: MQTTSourceConfig,
        dispatch_mode: Option<DispatchMode>,
        dispatch_buffer_capacity: Option<usize>,
    ) -> Result<Self> {
        let mut base_params = SourceBaseParams::new(id);
        if let Some(mode) = dispatch_mode {
            base_params = base_params.with_dispatch_mode(mode);
        }
        if let Some(capacity) = dispatch_buffer_capacity {
            base_params = base_params.with_dispatch_buffer_capacity(capacity);
        }
        Ok(Self {
            base: SourceBase::new(base_params)?,
            config,
        })
    }
}

#[async_trait]
impl Source for MQTTSource {
    fn id(&self) -> &str {
        &self.base.id
    }

    fn type_name(&self) -> &str {
        "mqtt"
    }

    fn properties(&self) -> std::collections::HashMap<String, serde_json::Value> {
        let mut props = HashMap::new();
        props
    }

    fn auto_start(&self) -> bool {
        self.base.get_auto_start()
    }

    async fn start(&self) -> Result<()> {
        info!("[{}] Starting MQTT source", self.id());

        self.base.set_status(ComponentStatus::Starting).await;
        self.base
            .send_component_event(
                ComponentStatus::Starting,
                Some("Starting MQTT Source".to_string()),
            )
            .await?;

        let mqtt_conn_config = self.config.to_mqtt_connection_config(self.id());

        let mut mqtt_connection = connection::MQTTConnectionWrapper::new(mqtt_conn_config);

        mqtt_connection.start().await?;

        self.base.set_status(ComponentStatus::Running).await;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Stopping MQTT source {}", self.id());

        self.base.set_status(ComponentStatus::Stopping).await;

        self.base.set_status(ComponentStatus::Stopped).await;

        Ok(())
    }

    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }

    async fn subscribe(
        &self,
        settings: drasi_lib::config::SourceSubscriptionSettings,
    ) -> Result<SubscriptionResponse> {
        self.base.subscribe_with_bootstrap(&settings, "MQTT").await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn deprovision(&self) -> Result<()> {
        Ok(())
    }

    async fn initialize(&self, context: SourceRuntimeContext) {
        self.base.initialize(context).await;
    }
}

pub struct MQTTSourceBuilder {
    id: String,
    host: String,
    port: u16,
    topic: String,
    qos: Option<QualityOfService>,
    username: Option<String>,
    password: Option<String>,
    dispatch_mode: Option<DispatchMode>,
    dispatch_buffer_capacity: Option<usize>,
    bootstrap_provider: Option<Box<dyn drasi_lib::bootstrap::BootstrapProvider + 'static>>,
    channel_capacity: Option<usize>,
    auto_start: bool,
    timeout_ms: Option<u64>,
}

impl MQTTSourceBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            host: String::new(),
            port: 9001,
            topic: String::new(),
            qos: None,
            username: None,
            password: None,
            dispatch_mode: None,
            dispatch_buffer_capacity: None,
            bootstrap_provider: None,
            channel_capacity: None,
            auto_start: true,
            timeout_ms: None,
        }
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }

    pub fn with_qos(mut self, qos: QualityOfService) -> Self {
        self.qos = Some(qos);
        self
    }

    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn with_dispatch_mode(mut self, mode: DispatchMode) -> Self {
        self.dispatch_mode = Some(mode);
        self
    }

    pub fn with_dispatch_buffer_capacity(mut self, capacity: usize) -> Self {
        self.dispatch_buffer_capacity = Some(capacity);
        self
    }

    pub fn with_bootstrap_provider(
        mut self,
        provider: Box<dyn drasi_lib::bootstrap::BootstrapProvider + 'static>,
    ) -> Self {
        self.bootstrap_provider = Some(provider);
        self
    }

    pub fn with_auto_start(mut self, auto_start: bool) -> Self {
        self.auto_start = auto_start;
        self
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_config(mut self, config: MQTTSourceConfig) -> Self {
        self.host = config.host;
        self.port = config.port;
        self.topic = config.topic;
        self.username = config.username;
        self.password = config.password;
        self.qos = Some(config.qos);
        self
    }

    pub fn build(self) -> Result<MQTTSource> {
        let config = MQTTSourceConfig {
            host: self.host,
            port: self.port,
            topic: self.topic,
            username: self.username,
            password: self.password,
            qos: self.qos.unwrap_or_else(|| default_qos()),
            channel_capacity: self
                .channel_capacity
                .unwrap_or_else(|| default_channel_capacity()),
            timeout_ms: self.timeout_ms.unwrap_or_else(|| default_timeout_ms()),
        };
        MQTTSource::with_dispatch(
            self.id,
            config,
            self.dispatch_mode,
            self.dispatch_buffer_capacity,
        )
    }
}

impl MQTTSource {
    pub fn builder(id: impl Into<String>) -> MQTTSourceBuilder {
        MQTTSourceBuilder::new(id)
    }
}
