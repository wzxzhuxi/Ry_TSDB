use std::collections::HashMap;
use serde::{Serialize, Deserialize};

pub type Timestamp = u64;
pub type TagValue = String;
pub type FieldValue = f64;

/// 表示时序数据的单个数据点，包含时间戳、标签集和字段值集
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataPoint {
    /// 数据点的时间戳
    pub timestamp: Timestamp,
    /// 标签集合 - 用于数据分类和过滤，如host=server1, region=us-west
    pub tags: HashMap<String, TagValue>,
    /// 字段值集合 - 实际测量的数据，如cpu_usage=0.45, memory_used=1024.5
    pub fields: HashMap<String, FieldValue>,
}

impl DataPoint {
    pub fn new(timestamp: Timestamp) -> Self {
        DataPoint {
            timestamp,
            tags: HashMap::new(),
            fields: HashMap::new(),
        }
    }
    
    pub fn add_tag(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.tags.insert(key.into(), value.into());
        self
    }
    
    pub fn add_field(&mut self, key: impl Into<String>, value: f64) -> &mut Self {
        self.fields.insert(key.into(), value);
        self
    }
}

/// 表示一个时间序列，由一组标签唯一标识
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SeriesKey {
    /// 衡量什么的名称，如"cpu_usage"、"temperature"
    pub measurement: String,
    /// 标签集合，如host=server1, region=us-west
    pub tags: HashMap<String, TagValue>,
}

// 手动实现Hash trait，因为HashMap不能自动派生Hash
impl std::hash::Hash for SeriesKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.measurement.hash(state);
        // 对标签排序，确保相同内容的标签集合生成相同的哈希值
        let mut sorted_tags: Vec<(&String, &TagValue)> = self.tags.iter().collect();
        sorted_tags.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in sorted_tags {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl SeriesKey {
    pub fn new(measurement: impl Into<String>) -> Self {
        SeriesKey {
            measurement: measurement.into(),
            tags: HashMap::new(),
        }
    }
    
    pub fn add_tag(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.tags.insert(key.into(), value.into());
        self
    }
    
    /// 创建一个规范形式的字符串表示，用于一致性哈希
    pub fn to_canonical_string(&self) -> String {
        let mut pairs: Vec<(&String, &String)> = self.tags.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        
        let mut result = self.measurement.clone();
        for (k, v) in pairs {
            result.push_str(&format!(",{}={}", k, v));
        }
        result
    }
}

/// 表示查询过滤条件
#[derive(Clone, Debug)]
pub struct QueryFilter {
    /// 要查询的测量名称
    pub measurement: Option<String>,
    /// 时间范围
    pub time_range: (Timestamp, Timestamp),
    /// 标签过滤条件，如 {"host": "server1", "region": "us-west"}
    pub tags: HashMap<String, TagValue>,
    /// 要返回的字段，如果为空则返回所有字段
    pub fields: Vec<String>,
}

impl QueryFilter {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        QueryFilter {
            measurement: None,
            time_range: (start, end),
            tags: HashMap::new(),
            fields: Vec::new(),
        }
    }
    
    pub fn measurement(mut self, measurement: impl Into<String>) -> Self {
        self.measurement = Some(measurement.into());
        self
    }
    
    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
    
    pub fn add_field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
        self
    }
}

