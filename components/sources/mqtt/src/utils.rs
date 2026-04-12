use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct MqttPacket {
    pub topic: String,
    pub payload: Bytes,
    pub timestamp: u64,
}
