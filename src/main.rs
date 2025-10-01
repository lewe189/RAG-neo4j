use neo4j::{Neo4jService, Config};
use clap::{Parser, Subcommand};
use std::path::Path;

#[derive(Parser)]
#[command(name = "neo4j-tool")]
#[command(about = "Neo4j数据导入导出工具")]
struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 导入数据到Neo4j数据库
    Import {
        /// 输入文件路径（可选，未指定时使用配置文件中的默认路径）
        #[arg(short, long)]
        input: Option<String>,
    },
    
    /// 执行Cypher脚本文件
    Cypher {
        /// Cypher脚本文件路径
        #[arg(short, long)]
        file: String,
    },
    
    /// 清空数据库（开发阶段用）
    Clear,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // 从配置文件加载配置
    let config = Config::from_file(&cli.config).map_err(|e| {
        format!("无法加载配置文件 '{}': {}", cli.config, e)
    })?;
    
    // 验证配置
    config.validate().map_err(|e| {
        format!("配置验证失败: {}", e)
    })?;
    
    // 使用配置中的Neo4j连接信息
    let neo4j_config = &config.neo4j;
    let uri = neo4j_config.get_connection_uri();
    let user = &neo4j_config.user;
    let password = &neo4j_config.password;

    let service = Neo4jService::new(&uri, user, password).await?;
    println!("连接成功！URI: {}, 用户: {}", uri, user);

    match &cli.command {
        Commands::Import { input } => {
            println!("=== 程序启动 ===");
            
            // 确定输入路径：优先使用命令行参数，否则使用配置文件中的默认路径
            let input_path = match input {
                Some(path) => path.clone(),
                None => {
                    match &config.input {
                        Some(input_config) => {
                            input_config.path.as_ref()
                                .ok_or("配置文件中未指定输入路径，且命令行也未提供输入路径")?
                                .clone()
                        },
                        None => return Err("未找到输入配置段，且命令行也未提供输入路径".into()),
                    }
                }
            };

            // === BEFORE_RUN 执行 ===
            println!("=== 开始执行Before_Run配置 ===");
            if let Some(before_run) = &config.before_run {
                // 1. 验证TOML文件格式（如果启用）
                if before_run.verify_toml.unwrap_or(false) {
                    println!("正在验证TOML文件格式...");
                    
                    if Path::new(&input_path).is_dir() {
                        // 验证目录中的所有TOML文件
                        if let Err(e) = Neo4jService::verify_toml_directory(&input_path) {
                            println!("=== TOML文件验证失败 ===");
                            println!("错误: {}", e);
                            println!("=== 程序因验证失败而终止 ===");
                            return Err(e);
                        }
                    } else {
                        // 验证单个TOML文件
                        if let Err(e) = Neo4jService::verify_toml_file(&input_path) {
                            println!("=== TOML文件验证失败 ===");
                            println!("错误: {}", e);
                            println!("=== 程序因验证失败而终止 ===");
                            return Err(e);
                        }
                    }
                    
                    println!("TOML文件格式验证通过");
                } else {
                    println!("跳过TOML文件格式验证（根据配置）");
                }

                // 2. 清空数据库（如果启用）
                if before_run.clear_db.unwrap_or(false) {
                    println!("正在根据配置清空数据库...");
                    service.clear_database().await?;
                    println!("数据库已清空");
                } else {
                    println!("跳过数据库清空（根据配置）");
                }
            } else {
                println!("未找到Before_Run配置，跳过预处理步骤");
            }
            println!("=== Before_Run执行完成 ===");

            // === 主要处理逻辑 ===
            println!("=== 开始主要数据处理 ===");

            // 获取线程数配置
            let max_concurrent = config.run
                .as_ref()
                .and_then(|run_config| run_config.threads)
                .unwrap_or(1) as usize; // 默认单线程

            if Path::new(&input_path).is_dir() {
                // 如果是目录，使用完整配置进行节点优先处理
                println!("开始递归扫描目录: {} (并发数: {})", input_path, max_concurrent);
                service.load_directory_with_config(&input_path, &config).await?;
                println!("已从目录 {} 递归导入所有TOML文件", input_path);
            } else {
                // 如果是单个文件，加载单个TOML文件
                service.load_from_toml_file(&input_path).await?;
                println!("已从文件 {} 导入TOML数据", input_path);
            }
            
            println!("=== 主要数据处理完成 ===");

            // === AFTER_RUN 执行 ===
            if let Some(after_run) = &config.after_run {
                service.execute_after_run(after_run).await?;
            } else {
                println!("未找到After_Run配置，跳过后处理步骤");
            }
        },
        
        Commands::Cypher { file } => {
            service.execute_cypher_file(file).await?;
            println!("已执行Cypher脚本文件 {}", file);
        },
        
        Commands::Clear => {
            service.clear_database().await?;
            println!("数据库已清空");
        },
    }
    
    Ok(())
}

