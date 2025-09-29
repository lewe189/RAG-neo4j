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
        /// 输入文件路径
        #[arg(short, long)]
        input: String,
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

                    if Path::new(input).is_dir() {
                        // 如果是目录，加载目录中的所有TOML文件
                        service.load_directory(input).await?;
                        println!("已从目录 {} 导入所有TOML文件", input);
                    } else {
                        // 如果是单个文件，加载单个TOML文件
                        service.load_from_toml_file(input).await?;
                        println!("已从文件 {} 导入TOML数据", input);
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

