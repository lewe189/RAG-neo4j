use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use toml::Value;

// 动态TOML数据结构
/// 表示一个节点实体，包含节点类型和属性
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeEntity {
    /// 节点类型（从TOML表头获取，如"教师"、"论文"等）
    pub node_type: String,
    /// 节点的所有属性（键值对）
    pub properties: HashMap<String, PropertyValue>,
}

/// 表示一个关系实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationEntity {
    /// 源节点标识
    pub source: String,
    /// 目标节点标识
    pub target: String,
    /// 关系类型
    pub relation_type: String,
    /// 关系的属性（可选）
    pub properties: HashMap<String, PropertyValue>,
}

/// 属性值的枚举类型，支持多种数据类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<String>), // 简化为字符串数组
}

impl PropertyValue {
    /// 从TOML Value转换为PropertyValue
    pub fn from_toml_value(value: &Value) -> Self {
        match value {
            Value::String(s) => PropertyValue::String(s.clone()),
            Value::Integer(i) => PropertyValue::Integer(*i),
            Value::Float(f) => PropertyValue::Float(*f),
            Value::Boolean(b) => PropertyValue::Boolean(*b),
            Value::Array(arr) => {
                let string_array: Vec<String> = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .collect();
                PropertyValue::Array(string_array)
            },
            _ => PropertyValue::String(value.to_string()),
        }
    }

    /// 将PropertyValue转换为字符串（用于Neo4j查询）
    pub fn to_string(&self) -> String {
        match self {
            PropertyValue::String(s) => s.clone(),
            PropertyValue::Integer(i) => i.to_string(),
            PropertyValue::Float(f) => f.to_string(),
            PropertyValue::Boolean(b) => b.to_string(),
            PropertyValue::Array(arr) => arr.join(", "), // 移除方括号，直接用逗号分隔
        }
    }
}

/// 动态解析的TOML数据结构
#[derive(Debug, Clone)]
pub struct DynamicTomlData {
    /// 所有的节点实体
    pub nodes: Vec<NodeEntity>,
    /// 所有的关系实体
    pub relations: Vec<RelationEntity>,
}

impl DynamicTomlData {
    /// 从TOML动态解析数据
    pub fn from_toml_str(toml_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let toml_value: Value = toml::from_str(toml_str)?;
        
        let mut nodes = Vec::new();
        let mut relations = Vec::new();

        if let Value::Table(root_table) = toml_value {
            for (table_name, table_value) in root_table {
                if let Value::Array(items) = table_value {
                    // 处理节点类型的表（如教师、论文等）
                    if table_name == "关系" {
                        // 特殊处理关系表
                        for item in items {
                            if let Value::Table(relation_table) = item {
                                let relation = Self::parse_relation(&relation_table)?;
                                relations.push(relation);
                            }
                        }
                    } else {
                        // 处理普通节点表
                        for item in items {
                            if let Value::Table(node_table) = item {
                                let node = Self::parse_node(&table_name, &node_table);
                                nodes.push(node);
                            }
                        }
                    }
                }
            }
        }

        Ok(DynamicTomlData { nodes, relations })
    }

    /// 解析单个节点
    fn parse_node(node_type: &str, table: &toml::map::Map<String, Value>) -> NodeEntity {
        let mut properties = HashMap::new();
        
        for (key, value) in table {
            properties.insert(key.clone(), PropertyValue::from_toml_value(value));
        }

        NodeEntity {
            node_type: node_type.to_string(),
            properties,
        }
    }

    /// 解析关系
    fn parse_relation(table: &toml::map::Map<String, Value>) -> Result<RelationEntity, Box<dyn std::error::Error>> {
        let source = table.get("源实体标识")
            .and_then(|v| v.as_str())
            .ok_or("关系缺少'源实体标识'字段")?
            .to_string();
            
        let target = table.get("目标实体标识")
            .and_then(|v| v.as_str())
            .ok_or("关系缺少'目标实体标识'字段")?
            .to_string();
            
        let relation_type = table.get("关系类型")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                table.get("关系名称")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN_RELATION")
            })
            .to_string();

        let mut properties = HashMap::new();
        
        // 将其他字段作为关系属性（排除关系标识字段）
        for (key, value) in table {
            if !["源实体标识", "目标实体标识", "关系类型", "关系名称"].contains(&key.as_str()) {
                properties.insert(key.clone(), PropertyValue::from_toml_value(value));
            }
        }

        Ok(RelationEntity {
            source,
            target,
            relation_type,
            properties,
        })
    }

    /// 从文件路径加载TOML数据
    pub fn from_file(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let toml_str = std::fs::read_to_string(file_path)?;
        Self::from_toml_str(&toml_str)
    }
}




