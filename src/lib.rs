use neo4rs::*;
use std::fs;
use std::path::{Path, PathBuf};
use futures::stream::{self, StreamExt};

// 导入新的代数数据类型
pub mod adt;
pub use adt::{DynamicTomlData, NodeEntity, RelationEntity, PropertyValue};

// 导入配置模块
pub mod config;
pub use config::{Config, Neo4jConfig};

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

    //支持分块处理的文件加载方法
    pub async fn load_from_toml_file_chunked<P: AsRef<Path>>(&self, file_path: P, chunk_size: usize) -> Result<(), Box<dyn std::error::Error>> {
        let dynamic_data = DynamicTomlData::from_file(file_path.as_ref().to_str().unwrap())?;
        
        println!("文件 {:?} 包含 {} 个节点, {} 个关系", 
                 file_path.as_ref(), 
                 dynamic_data.nodes.len(), 
                 dynamic_data.relations.len());
        
        // 如果数据量较小，直接处理
        if dynamic_data.nodes.len() + dynamic_data.relations.len() < chunk_size {
            return self.create_nodes_from_dynamic_data(&dynamic_data).await;
        }
        
        // 大文件分块并发处理
        self.create_nodes_from_dynamic_data_chunked(&dynamic_data, chunk_size).await?;
        
        Ok(())
    }

    // 验证TOML文件格式是否正确
    pub fn verify_toml_file<P: AsRef<Path>>(file_path: P) -> Result<(), Box<dyn std::error::Error>> {
        let path = file_path.as_ref();
        println!("验证TOML文件: {:?}", path);
        
        // 尝试读取并解析TOML文件
        let content = fs::read_to_string(path)
            .map_err(|e| format!("无法读取文件 {:?}: {}", path, e))?;
        
        // 验证TOML语法是否正确
        let _toml_value: toml::Value = toml::from_str(&content)
            .map_err(|e| format!("TOML语法错误 {:?}: {}", path, e))?;
        
        // 尝试解析为我们的动态数据结构
        match DynamicTomlData::from_file(path.to_str().unwrap()) {
            Ok(_) => {
                println!("TOML文件格式验证通过: {:?}", path);
                Ok(())
            },
            Err(e) => {
                Err(format!("TOML文件结构验证失败 {:?}: {}", path, e).into())
            }
        }
    }

    // 验证目录中所有TOML文件的格式
    pub fn verify_toml_directory<P: AsRef<Path>>(dir_path: P) -> Result<(), Box<dyn std::error::Error>> {
        let mut toml_files = Vec::new();
        Self::collect_toml_files_for_verification(dir_path.as_ref(), &mut toml_files)?;
        
        if toml_files.is_empty() {
            println!("未找到任何TOML文件需要验证");
            return Ok(());
        }
        
        println!("开始验证 {} 个TOML文件...", toml_files.len());
        
        let mut failed_files = Vec::new();
        
        for file_path in &toml_files {
            match Self::verify_toml_file(file_path) {
                Ok(_) => {
                    // 验证成功，继续
                },
                Err(e) => {
                    println!("验证失败: {}", e);
                    failed_files.push(file_path.clone());
                }
            }
        }
        
        if failed_files.is_empty() {
            println!("所有 {} 个TOML文件验证通过", toml_files.len());
            Ok(())
        } else {
            Err(format!("有 {} 个TOML文件验证失败: {:?}", failed_files.len(), failed_files).into())
        }
    }

    // 收集TOML文件用于验证（同步版本）
    fn collect_toml_files_for_verification(dir_path: &Path, toml_files: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
        let entries = fs::read_dir(dir_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                // 递归处理子目录
                Self::collect_toml_files_for_verification(&path, toml_files)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                toml_files.push(path);
            }
        }
        
        Ok(())
    }

    // 执行After_Run配置
    pub async fn execute_after_run(&self, after_run: &crate::config::AfterRunConfig) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== 开始执行After_Run配置 ===");
        
        // 执行Cypher脚本文件（如果指定）
        if let Some(cypher_file) = &after_run.cypher_file {
            if !cypher_file.trim().is_empty() {
                if Path::new(cypher_file).exists() {
                    println!("执行After_Run Cypher脚本: {}", cypher_file);
                    self.execute_cypher_file(cypher_file).await?;
                    println!("After_Run Cypher脚本执行完成");
                } else {
                    println!("指定的Cypher脚本文件不存在: {}", cypher_file);
                }
            }
        }
        
        // 输出最终状态（如果启用）
        if after_run.output_status.unwrap_or(false) {
            println!("=== 最终状态: OK ===");
        }
        
        println!("=== After_Run执行完成 ===");
        Ok(())
    }

    // 递归加载文件夹中的所有TOML文件（支持并发处理）
    pub async fn load_directory_with_config<P: AsRef<Path>>(&self, dir_path: P, max_concurrent: usize) -> Result<(), Box<dyn std::error::Error>> {
        self.load_directory_recursive_concurrent(dir_path.as_ref(), max_concurrent).await
    }

    // 递归加载文件夹中的所有TOML文件（默认单线程，保持向后兼容）
    pub async fn load_directory<P: AsRef<Path>>(&self, dir_path: P) -> Result<(), Box<dyn std::error::Error>> {
        self.load_directory_recursive_concurrent(dir_path.as_ref(), 1).await
    }

    // 递归扫描目录并并发加载所有TOML文件
    async fn load_directory_recursive_concurrent(&self, dir_path: &Path, max_concurrent: usize) -> Result<(), Box<dyn std::error::Error>> {
        // 收集所有TOML文件路径
        let mut toml_files = Vec::new();
        let mut subdirs = Vec::new();
        
        self.collect_files_recursive(dir_path, &mut toml_files, &mut subdirs)?;
        
        println!("发现 {} 个TOML文件，准备并发处理 (最大并发数: {})", toml_files.len(), max_concurrent);
        
        if toml_files.is_empty() {
            println!("没有找到任何TOML文件");
            return Ok(());
        }
        
        // 预估文件大小，按大小排序（大文件优先处理）
        let mut file_info: Vec<_> = toml_files.iter()
            .map(|path| {
                let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                (path.clone(), size)
            })
            .collect();
        
        // 大文件优先排序
        file_info.sort_by(|a, b| b.1.cmp(&a.1));
        
        println!("文件大小分布:");
        for (path, size) in &file_info {
            println!("  {:?}: {} bytes", path.file_name().unwrap_or_default(), size);
        }
        
        // 使用流式处理控制并发，避免生命周期问题
        
        let results = stream::iter(file_info.into_iter())
            .map(|(file_path, size)| async move {
                println!("开始加载文件: {:?}", file_path.file_name().unwrap_or_default());
                
                // 根据文件大小决定处理策略
                let chunk_size = if size > 1_000_000 { // 1MB以上的文件
                    1000 // 使用较小的块大小
                } else if size > 100_000 { // 100KB以上的文件
                    5000 // 使用中等的块大小
                } else {
                    10000 // 小文件使用大块大小
                };
                
                
                let result = if size > 500_000 { // 500KB以上使用分块处理
                    self.load_from_toml_file_chunked(&file_path, chunk_size).await
                } else {
                    self.load_from_toml_file(&file_path).await
                };
                

                
                match result {
                    Ok(_) => {
                        println!("完成文件加载: {:?}", file_path.file_name().unwrap_or_default());
                        Ok(())
                    },
                    Err(e) => {
                        println!("文件加载失败: {:?} - 错误: {}", file_path.file_name().unwrap_or_default(), e);
                        Err(e)
                    }
                }
            })
            .buffer_unordered(max_concurrent) // 控制最大并发数
            .collect::<Vec<_>>()
            .await;
        
        // 收集所有错误
        let mut errors = Vec::new();
        for result in results {
            if let Err(e) = result {
                errors.push(e);
            }
        }
        
        if !errors.is_empty() {
            println!("处理过程中发生 {} 个错误", errors.len());
            // 返回第一个错误
            return Err(errors.into_iter().next().unwrap());
        }
        
        println!("所有TOML文件处理完成");
        Ok(())
    }
    
    // 递归收集所有TOML文件路径
    fn collect_files_recursive(&self, dir_path: &Path, toml_files: &mut Vec<PathBuf>, subdirs: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
        let entries = fs::read_dir(dir_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                subdirs.push(path.clone());
                // 递归处理子目录
                self.collect_files_recursive(&path, toml_files, subdirs)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                toml_files.push(path);
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

    //分块并发处理大型数据
    async fn create_nodes_from_dynamic_data_chunked(&self, dynamic_data: &DynamicTomlData, chunk_size: usize) -> Result<(), Box<dyn std::error::Error>> {
        
        // 处理节点 - 分块并发
        if !dynamic_data.nodes.is_empty() {
            let node_chunks: Vec<_> = dynamic_data.nodes.chunks(chunk_size).collect();
            
            let results = stream::iter(node_chunks.into_iter().enumerate())
                .map(|(i, chunk)| {
                    let chunk_data = DynamicTomlData {
                        nodes: chunk.to_vec(),
                        relations: Vec::new(),
                    };
                    async move {
                        println!("处理节点块 {}", i + 1);
                        self.create_dynamic_nodes_batch(&chunk_data).await
                    }
                })
                .buffer_unordered(4) // 同时处理4个块
                .collect::<Vec<_>>()
                .await;
                
            for result in results {
                result?;
            }
        }
        
        // 处理关系 - 分块并发
        if !dynamic_data.relations.is_empty() {
            let relation_chunks: Vec<_> = dynamic_data.relations.chunks(chunk_size).collect();

            let results = stream::iter(relation_chunks.into_iter().enumerate())
                .map(|(i, chunk)| {
                    let chunk_data = DynamicTomlData {
                        nodes: Vec::new(),
                        relations: chunk.to_vec(),
                    };
                    async move {
                        println!("处理关系块 {}", i + 1);
                        self.create_dynamic_relations_batch(&chunk_data).await
                    }
                })
                .buffer_unordered(4) // 同时处理4个块
                .collect::<Vec<_>>()
                .await;
                
            for result in results {
                result?;
            }
        }
        
        println!("分块处理完成");
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
}
    
