
use drasi_core::evaluation::variable_value::de;
use matchit::Router;

use crate::config::TopicMapping;

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
}