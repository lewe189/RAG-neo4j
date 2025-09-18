use neo4rs::*;
use std::fs;
use std::path::Path;

// 导入新的代数数据类型
pub mod adt;
pub use adt::{DynamicTomlData, NodeEntity, RelationEntity, PropertyValue};

// 定义一个结构化的Neo4j服务 OOP
pub struct Neo4jService {
    graph: Graph,
}

impl Neo4jService {
    pub async fn new(uri: &str, user: &str, password: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let graph = Graph::new(uri, user, password).await?;
        Ok(Self { graph })
    }

    // 清空数据库
    pub async fn clear_database(&self) -> Result<(), Box<dyn std::error::Error>> {
        let query = query("MATCH (n) DETACH DELETE n");
        self.graph.run(query).await?;
        Ok(())
    }

    // 执行Cypher脚本文件
    pub async fn execute_cypher_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(file_path.as_ref())?;
        
        // 按分号分割Cypher语句，并过滤掉注释
        let cleaned_content = content
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with("//"))
            .collect::<Vec<&str>>()
            .join(" ");
            
        let statements: Vec<&str> = cleaned_content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for (i, statement) in statements.iter().enumerate() {
            if !statement.is_empty() {
                let cypher_query = query(statement);
                match self.graph.run(cypher_query).await {
                    Ok(_) => println!("执行成功 #{}: {}", i+1, statement.chars().take(80).collect::<String>()),
                    Err(e) => println!("执行失败 #{}: {} - 错误: {}", i+1, statement.chars().take(80).collect::<String>(), e),
                }
            }
        }
        
        Ok(())
    }

    // 从TOML文件加载数据
    pub async fn load_from_toml_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), Box<dyn std::error::Error>> {
        let dynamic_data = DynamicTomlData::from_file(file_path.as_ref().to_str().unwrap())?;
        
        // 处理动态数据并创建到数据库
        self.create_nodes_from_dynamic_data(&dynamic_data).await?;
        
        Ok(())
    }

    // 加载文件夹中的所有文件（仅当前目录，不递归）
    pub async fn load_directory<P: AsRef<Path>>(&self, dir_path: P) -> Result<(), Box<dyn std::error::Error>> {
        self.load_directory_internal(dir_path, false, 0).await
    }

    // 递归加载文件夹中的所有文件（包括子文件夹）
    pub async fn load_directory_recursive<P: AsRef<Path>>(&self, dir_path: P) -> Result<(), Box<dyn std::error::Error>> {
        println!("开始递归扫描目录: {:?}", dir_path.as_ref());
        let result = self.load_directory_internal(dir_path, true, 0).await;
        println!("递归扫描完成");
        result
    }

    // 内部实现：支持递归和非递归的目录加载
    async fn load_directory_internal<P: AsRef<Path>>(&self, dir_path: P, recursive: bool, depth: usize) -> Result<(), Box<dyn std::error::Error>> {
        let path = dir_path.as_ref();
        
        // 防止过深的递归
        if depth > 50 {
            println!("警告: 目录层级过深 ({}), 跳过: {:?}", depth, path);
            return Ok(());
        }

        // 添加缩进以显示目录层级
        let indent = "  ".repeat(depth);
        
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(e) => {
                println!("{}无法读取目录 {:?}: {}", indent, path, e);
                return Err(e.into());
            }
        };
        
        let mut toml_files = Vec::new();
        let mut subdirs = Vec::new();
        
        // 收集所有条目
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    println!("{}跳过无法读取的条目: {}", indent, e);
                    continue;
                }
            };
            
            let entry_path = entry.path();
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(e) => {
                    println!("{}跳过无法获取元数据的条目 {:?}: {}", indent, entry_path, e);
                    continue;
                }
            };
            
            if metadata.is_file() {
                if entry_path.extension().and_then(|s| s.to_str()) == Some("toml") {
                    toml_files.push(entry_path);
                }
            } else if metadata.is_dir() && recursive {
                subdirs.push(entry_path);
            }
        }
        
        // 处理当前目录的TOML文件
        if !toml_files.is_empty() {
            println!("{}在目录 {:?} 中发现 {} 个TOML文件", indent, path, toml_files.len());
            
            for toml_file in toml_files {
                println!("{}开始加载文件: {:?}", indent, toml_file);
                match self.load_from_toml_file(&toml_file).await {
                    Ok(_) => {
                        println!("{}✓ 完成文件加载: {:?}", indent, toml_file);
                    },
                    Err(e) => {
                        println!("{}✗ 文件加载失败: {:?} - 错误: {}", indent, toml_file, e);
                        // 继续处理其他文件，不中断整个流程
                    }
                }
            }
        }
        
        // 递归处理子目录
        if recursive && !subdirs.is_empty() {
            println!("{}在目录 {:?} 中发现 {} 个子目录", indent, path, subdirs.len());
            
            for subdir in subdirs {
                println!("{}进入子目录: {:?}", indent, subdir);
                if let Err(e) = self.load_directory_internal(&subdir, recursive, depth + 1).await {
                    println!("{}✗ 处理子目录失败: {:?} - 错误: {}", indent, subdir, e);
                    // 继续处理其他子目录，不中断整个流程
                }
            }
        }
        
        Ok(())
    }

    // 根据动态TOML数据创建节点到数据库
    async fn create_nodes_from_dynamic_data(&self, dynamic_data: &DynamicTomlData) -> Result<(), Box<dyn std::error::Error>> {
        // 批量处理所有节点
        self.create_dynamic_nodes_batch(dynamic_data).await?;
        
        // 批量处理所有关系
        self.create_dynamic_relations_batch(dynamic_data).await?;
        
        Ok(())
    }

    // 批量创建节点
    async fn create_dynamic_nodes_batch(&self, dynamic_data: &DynamicTomlData) -> Result<(), Box<dyn std::error::Error>> {
        if dynamic_data.nodes.is_empty() {
            return Ok(());
        }

        println!("开始批量处理 {} 个节点...", dynamic_data.nodes.len());
        
        // 按节点类型分组
        let mut nodes_by_type: std::collections::HashMap<&str, Vec<&NodeEntity>> = std::collections::HashMap::new();
        
        for node in &dynamic_data.nodes {
            // 检查标识符
            if let Some(PropertyValue::String(id)) = node.properties.get("标识符") {
                if !id.trim().is_empty() {
                    nodes_by_type.entry(&node.node_type)
                        .or_insert_with(Vec::new)
                        .push(node);
                }
            } else {
                println!("跳过没有标识符的节点: {}", node.node_type);
            }
        }

        if nodes_by_type.is_empty() {
            println!("没有有效的节点数据需要处理");
            return Ok(());
        }

        // 为每种节点类型执行批量操作
        let mut total_processed = 0;
        for (node_type, type_nodes) in nodes_by_type {
            let node_data: Vec<std::collections::HashMap<&str, String>> = type_nodes.iter()
                .filter_map(|node| {
                    let mut node_map = std::collections::HashMap::new();
                    
                    // 添加所有属性
                    for (key, value) in &node.properties {
                        // 跳过空值
                        match value {
                            PropertyValue::String(s) if s.trim().is_empty() || s == "" => continue,
                            PropertyValue::Array(arr) if arr.is_empty() => continue,
                            _ => {}
                        }
                        node_map.insert(key.as_str(), value.to_string());
                    }
                    
                    if node_map.is_empty() {
                        None
                    } else {
                        Some(node_map)
                    }
                })
                .collect();
            
            if node_data.is_empty() {
                continue;
            }
            
            let cypher_query = format!(
                r#"
                UNWIND $nodes AS nodeData
                MERGE (n:{} {{标识符: nodeData.标识符}})
                SET n += nodeData
                RETURN count(n) as processed_count
                "#,
                node_type
            );
            
            let query_builder = query(&cypher_query).param("nodes", node_data);
            let processed_count = type_nodes.len();
            
            match self.graph.run(query_builder).await {
                Ok(_) => {
                    total_processed += processed_count;
                    println!("成功批量处理 {} 个 {} 节点", processed_count, node_type);
                },
                Err(e) => {
                    println!("批量处理 {} 节点失败: {}", node_type, e);
                    // 单个节点类型失败时，回退到逐个处理
                    return self.create_dynamic_nodes_fallback(dynamic_data).await;
                }
            }
        }
        
        println!("成功批量处理总计 {} 个节点", total_processed);
        
        Ok(())
    }

    // 保守方案：逐个创建节点
    async fn create_dynamic_nodes_fallback(&self, dynamic_data: &DynamicTomlData) -> Result<(), Box<dyn std::error::Error>> {
        for node in &dynamic_data.nodes {
            // 获取标识符
            let identifier = match node.properties.get("标识符") {
                Some(PropertyValue::String(id)) if !id.trim().is_empty() => id,
                _ => {
                    println!("跳过没有标识符的节点: {}", node.node_type);
                    continue;
                }
            };

            // 直接构建查询，最小化内存分配
            let query_builder = query(&format!(
                "MERGE (n:{} {{标识符: $identifier}}) SET n.标识符 = $identifier",
                node.node_type
            )).param("identifier", identifier.as_str());
            
            // 添加其他属性
            for (key, value) in &node.properties {
                if key == "标识符" {
                    continue;
                }
                
                match value {
                    PropertyValue::String(s) if s.trim().is_empty() || s == "" => continue,
                    PropertyValue::Array(arr) if arr.is_empty() => continue,
                    _ => {}
                }
                
                // 为每个属性执行单独的SET操作，避免复杂的参数处理
                let set_query = format!(
                    "MATCH (n:{} {{标识符: $identifier}}) SET n.{} = $value",
                    node.node_type, key
                );
                
                let value_str = value.to_string();
                let attr_query = query(&set_query)
                    .param("identifier", identifier.as_str())
                    .param("value", value_str.as_str());
                
                if let Err(e) = self.graph.run(attr_query).await {
                    println!("设置属性失败: {} - {}", key, e);
                }
            }
            
            match self.graph.run(query_builder).await {
                Ok(_) => {
                    // 静默处理
                },
                Err(e) => {
                    println!("处理节点失败: {} (标识符: {}) - 错误: {}", node.node_type, identifier, e);
                }
            }
        }
        
        println!("完成回退方案处理 {} 个节点", dynamic_data.nodes.len());
        Ok(())
    }

    // 批量创建关系
    async fn create_dynamic_relations_batch(&self, dynamic_data: &DynamicTomlData) -> Result<(), Box<dyn std::error::Error>> {
        if dynamic_data.relations.is_empty() {
            return Ok(());
        }

        println!("开始批量处理 {} 个关系...", dynamic_data.relations.len());

        // 按关系类型分组
        let mut relations_by_type: std::collections::HashMap<String, Vec<&RelationEntity>> = std::collections::HashMap::new();
        
        for relation in &dynamic_data.relations {
            let sanitized_type = Self::sanitize_relation_type(&relation.relation_type);
            relations_by_type.entry(sanitized_type)
                .or_insert_with(Vec::new)
                .push(relation);
        }

        if relations_by_type.is_empty() {
            println!("没有有效的关系数据需要处理");
            return Ok(());
        }

        // 为每种关系类型执行批量操作
        let mut total_processed = 0;
        for (rel_type, type_relations) in relations_by_type {
            // 检查是否有额外属性需要设置
            let has_extra_props = type_relations.iter()
                .any(|rel| !rel.properties.is_empty());
            
            // 构建关系数据
            let relation_data: Vec<std::collections::HashMap<String, String>> = if has_extra_props {
                // 有额外属性时，分别构建标识符数据和属性数据
                type_relations.iter()
                    .map(|relation| {
                        let mut rel_map = std::collections::HashMap::new();
                        rel_map.insert("source_id".to_string(), relation.source.clone());
                        rel_map.insert("target_id".to_string(), relation.target.clone());
                        
                        // 添加关系属性
                        for (key, value) in &relation.properties {
                            match value {
                                PropertyValue::String(s) if s.trim().is_empty() || s == "" => continue,
                                PropertyValue::Array(arr) if arr.is_empty() => continue,
                                _ => {}
                            }
                            rel_map.insert(key.clone(), value.to_string());
                        }
                        
                        rel_map
                    })
                    .collect()
            } else {
                // 没有额外属性时，只包含标识符
                type_relations.iter()
                    .map(|relation| {
                        let mut rel_map = std::collections::HashMap::new();
                        rel_map.insert("source_id".to_string(), relation.source.clone());
                        rel_map.insert("target_id".to_string(), relation.target.clone());
                        rel_map
                    })
                    .collect()
            };
            
            let cypher_query = if has_extra_props {
                // 处理有额外属性的关系 - 手动构建SET子句
                let property_keys: std::collections::HashSet<String> = type_relations.iter()
                    .flat_map(|rel| rel.properties.keys())
                    .filter(|key| {
                        // 过滤掉空值属性
                        type_relations.iter().any(|rel| {
                            match rel.properties.get(*key) {
                                Some(PropertyValue::String(s)) if s.trim().is_empty() || s == "" => false,
                                Some(PropertyValue::Array(arr)) if arr.is_empty() => false,
                                None => false,
                                _ => true
                            }
                        })
                    })
                    .cloned()
                    .collect();
                
                let set_clauses: Vec<String> = property_keys.iter()
                    .map(|key| format!("r.{} = relData.{}", key, key))
                    .collect();
                
                if set_clauses.is_empty() {
                    format!(
                        r#"
                        UNWIND $relations AS relData
                        MATCH (source {{标识符: relData.source_id}})
                        MATCH (target {{标识符: relData.target_id}})
                        MERGE (source)-[r:{}]->(target)
                        RETURN count(r) as processed_count
                        "#,
                        rel_type
                    )
                } else {
                    format!(
                        r#"
                        UNWIND $relations AS relData
                        MATCH (source {{标识符: relData.source_id}})
                        MATCH (target {{标识符: relData.target_id}})
                        MERGE (source)-[r:{}]->(target)
                        SET {}
                        RETURN count(r) as processed_count
                        "#,
                        rel_type,
                        set_clauses.join(", ")
                    )
                }
            } else {
                format!(
                    r#"
                    UNWIND $relations AS relData
                    MATCH (source {{标识符: relData.source_id}})
                    MATCH (target {{标识符: relData.target_id}})
                    MERGE (source)-[r:{}]->(target)
                    RETURN count(r) as processed_count
                    "#,
                    rel_type
                )
            };
            
            let query_builder = query(&cypher_query).param("relations", relation_data);
            let processed_count = type_relations.len();
            
            match self.graph.run(query_builder).await {
                Ok(_) => {
                    total_processed += processed_count;
                    println!("成功批量处理 {} 个 {} 关系", processed_count, rel_type);
                },
                Err(e) => {
                    println!("批量处理 {} 关系失败: {}", rel_type, e);
                    // 单个关系类型失败时，回退到逐个处理
                    return self.create_dynamic_relations_fallback(dynamic_data).await;
                }
            }
        }
        
        println!("成功批量处理总计 {} 个关系", total_processed);
        
        Ok(())
    }

    // 保守方案：逐个创建关系
    async fn create_dynamic_relations_fallback(&self, dynamic_data: &DynamicTomlData) -> Result<(), Box<dyn std::error::Error>> {
        for relation in &dynamic_data.relations {
            // 先创建基本关系，然后添加属性
            let basic_query = format!(
                r#"
                MATCH (source {{标识符: $source_id}})
                MATCH (target {{标识符: $target_id}})
                MERGE (source)-[:{}]->(target)
                "#,
                Self::sanitize_relation_type(&relation.relation_type)
            );
            
            let query_builder = query(&basic_query)
                .param("source_id", relation.source.as_str())
                .param("target_id", relation.target.as_str());
            
            match self.graph.run(query_builder).await {
                Ok(_) => {
                    // 如果有额外属性，分别设置
                    for (key, value) in &relation.properties {
                        match value {
                            PropertyValue::String(s) if s.trim().is_empty() || s == "" => continue,
                            PropertyValue::Array(arr) if arr.is_empty() => continue,
                            _ => {}
                        }
                        
                        let attr_query = format!(
                            r#"
                            MATCH (source {{标识符: $source_id}})-[r:{}]->(target {{标识符: $target_id}})
                            SET r.{} = $value
                            "#,
                            Self::sanitize_relation_type(&relation.relation_type),
                            key
                        );
                        
                        let value_str = value.to_string();
                        let attr_query_builder = query(&attr_query)
                            .param("source_id", relation.source.as_str())
                            .param("target_id", relation.target.as_str())
                            .param("value", value_str.as_str());
                        
                        if let Err(e) = self.graph.run(attr_query_builder).await {
                            println!("设置关系属性失败: {} - {}", key, e);
                        }
                    }
                },
                Err(e) => {
                    println!("创建关系失败: {} -[{}]-> {} - 错误: {}", 
                        relation.source, relation.relation_type, relation.target, e);
                }
            }
        }
        
        println!("完成回退方案处理 {} 个关系", dynamic_data.relations.len());
        Ok(())
    }

    // 清理关系类型名称，确保符合Neo4j命名规范
    fn sanitize_relation_type(relation_type: &str) -> String {
        relation_type
            .replace(" ", "_")
            .replace("（", "_")
            .replace("）", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("-", "_")
            .replace("的", "_")
    }

    // 扫描目录结构，返回将要处理的TOML文件列表（不实际加载）
    pub async fn scan_directory<P: AsRef<Path>>(&self, dir_path: P, recursive: bool) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
        let mut toml_files = Vec::new();
        self.scan_directory_internal(dir_path, recursive, 0, &mut toml_files).await?;
        Ok(toml_files)
    }

    // 内部扫描实现
    async fn scan_directory_internal<P: AsRef<Path>>(&self, dir_path: P, recursive: bool, depth: usize, toml_files: &mut Vec<std::path::PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
        let path = dir_path.as_ref();
        
        // 防止过深的递归
        if depth > 50 {
            return Ok(());
        }

        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(e) => return Err(e.into()),
        };
        
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            
            let entry_path = entry.path();
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            
            if metadata.is_file() {
                if entry_path.extension().and_then(|s| s.to_str()) == Some("toml") {
                    toml_files.push(entry_path);
                }
            } else if metadata.is_dir() && recursive {
                self.scan_directory_internal(&entry_path, recursive, depth + 1, toml_files).await?;
            }
        }
        
        Ok(())
    }

    // 获取目录统计信息
    pub async fn get_directory_stats<P: AsRef<Path>>(&self, dir_path: P, recursive: bool) -> Result<DirectoryStats, Box<dyn std::error::Error>> {
        let toml_files = self.scan_directory(dir_path.as_ref(), recursive).await?;
        
        let mut stats = DirectoryStats {
            total_toml_files: toml_files.len(),
            directories_scanned: 0,
            deepest_level: 0,
            file_paths: toml_files.clone(),
        };
        
        // 计算目录统计
        let mut directories = std::collections::HashSet::new();
        let base_path = dir_path.as_ref();
        
        for file_path in &toml_files {
            if let Some(parent) = file_path.parent() {
                directories.insert(parent.to_path_buf());
                
                // 计算相对于基础路径的深度
                if let Ok(relative) = parent.strip_prefix(base_path) {
                    let depth = relative.components().count();
                    stats.deepest_level = stats.deepest_level.max(depth);
                }
            }
        }
        
        stats.directories_scanned = directories.len();
        
        Ok(stats)
    }
}

// 目录扫描统计信息
#[derive(Debug, Clone)]
pub struct DirectoryStats {
    pub total_toml_files: usize,
    pub directories_scanned: usize,
    pub deepest_level: usize,
    pub file_paths: Vec<std::path::PathBuf>,
}

impl DirectoryStats {
    pub fn print_summary(&self) {
        println!("=== 目录扫描统计 ===");
        println!("TOML文件总数: {}", self.total_toml_files);
        println!("扫描目录数: {}", self.directories_scanned);
        println!("最大层级深度: {}", self.deepest_level);
        
        if self.total_toml_files > 0 {
            println!("\n发现的文件:");
            for (i, path) in self.file_paths.iter().enumerate() {
                if i < 10 {  // 只显示前10个文件
                    println!("  {}: {:?}", i + 1, path);
                } else if i == 10 {
                    println!("  ... 还有 {} 个文件", self.total_toml_files - 10);
                    break;
                }
            }
        }
        println!("===================");
    }
}
    
