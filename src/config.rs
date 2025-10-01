use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(rename = "Neo4j")]
    pub neo4j: Neo4jConfig,
    
    #[serde(rename = "Before_Run")]
    pub before_run: Option<BeforeRunConfig>,
    
    #[serde(rename = "Run")]
    pub run: Option<RunConfig>,
    
    #[serde(rename = "After_Run")]
    pub after_run: Option<AfterRunConfig>,
    
    #[serde(rename = "Correction")]
    pub correction: Option<CorrectionConfig>,
    
    #[serde(rename = "Ignore")]
    pub ignore: Option<IgnoreConfig>,
    
    #[serde(rename = "APOC")]
    pub apoc: Option<ApocConfig>,
    
    #[serde(rename = "Input")]
    pub input: Option<InputConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Neo4jConfig {
    pub uri: String,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BeforeRunConfig {
    pub verify_toml: Option<bool>,
    pub clear_db: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RunConfig {
    pub count: Option<u32>,
    #[serde(rename = "Threads")]
    pub threads: Option<u32>,
    #[serde(rename = "ChunkConcurrency")]
    pub chunk_concurrency: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AfterRunConfig {
    pub cypher_file: Option<String>,
    pub output_status: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CorrectionConfig {
    pub case: Option<Vec<String>>,
    pub sanitize_node_labels: Option<bool>,
    pub sanitize_property_keys: Option<bool>,
    pub sanitize_relation_types: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IgnoreConfig {
    pub types: Option<Vec<String>>,
    pub propertys: Option<Vec<String>>,
    pub ignored_node_types: Option<Vec<String>>,
    pub ignored_relation_types: Option<Vec<String>>,
    pub ignored_node_properties: Option<Vec<String>>,
    pub ignored_relation_properties: Option<Vec<String>>,
    pub skip_empty_values: Option<bool>,
    pub filtered_values: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ApocConfig {
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct InputConfig {
    pub path: Option<String>,
    pub recursive_scan: Option<bool>,
}

impl Config {
    /// 从TOML文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
    
    
    /// 验证配置的必要字段
    pub fn validate(&self) -> Result<(), String> {
        if self.neo4j.uri.is_empty() {
            return Err("Neo4j URI不能为空".to_string());
        }
        if self.neo4j.user.is_empty() {
            return Err("Neo4j用户名不能为空".to_string());
        }
        if self.neo4j.password.is_empty() {
            return Err("Neo4j密码不能为空".to_string());
        }
        Ok(())
    }
}

impl Neo4jConfig {
    /// 获取完整的连接URI，确保包含协议前缀
    pub fn get_connection_uri(&self) -> String {
        if self.uri.starts_with("bolt://") || self.uri.starts_with("neo4j://") {
            self.uri.clone()
        } else {
            format!("bolt://{}", self.uri)
        }
    }
    
    /// 获取数据库名称，如果未指定则返回默认值
    pub fn get_database(&self) -> &str {
        self.database.as_deref().unwrap_or("neo4j")
    }
}