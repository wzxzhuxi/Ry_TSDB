use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use log::{info, error, debug};
use crate::db::SimpleTSDB;
use crate::error::Result;

/// TSDB网络服务器，处理TCP连接和命令
pub struct TsdbServer {
    db: Arc<SimpleTSDB>,
    addr: String,
}

impl TsdbServer {
    /// 创建新的服务器实例
    pub fn new(db: Arc<SimpleTSDB>, addr: String) -> Self {
        TsdbServer { db, addr }
    }
       
    /// 启动服务器并监听连接
    pub async fn run(&self) -> Result<()> {
        // 绑定到指定地址
        let listener = TcpListener::bind(&self.addr).await?;
        info!("TSDB服务器启动，监听 {}", self.addr);

        // 循环接受新连接
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("新连接：{}", addr);
                    
                    // 为每个连接创建一个任务
                    let db = Arc::clone(&self.db);
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(socket, db).await {
                            error!("处理连接错误: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("接受连接错误: {}", e);
                }
            }
        }
    }

    /// 处理单个客户端连接
    async fn handle_connection(mut socket: TcpStream, db: Arc<SimpleTSDB>) -> Result<()> {
        // 创建带缓冲的读取器
        let (reader, mut writer) = socket.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        // 循环读取命令
        while reader.read_line(&mut line).await? > 0 {
            debug!("收到命令: {}", line.trim());
            
            // 解析并处理命令
            let response = Self::process_command(&line, &db).await?;
            writer.write_all(response.as_bytes()).await?;
            
            // 清空缓冲区，准备读取下一行
            line.clear();
        }

        Ok(())
    }

    /// 处理命令并返回响应
    async fn process_command(cmd: &str, db: &SimpleTSDB) -> Result<String> {
        let parts: Vec<&str> = cmd.trim().split_whitespace().collect();
        
        if parts.is_empty() {
            return Ok("ERROR: 空命令\n".to_string());
        }

        match parts[0].to_uppercase().as_str() {
            "PUT" => {
                if parts.len() != 3 {
                    return Ok("ERROR: 格式错误，应为 PUT <timestamp> <value>\n".to_string());
                }
                
                let ts = match parts[1].parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Ok("ERROR: 时间戳必须是数字\n".to_string()),
                };
                
                let value = match parts[2].parse::<f64>() {
                    Ok(val) => val,
                    Err(_) => return Ok("ERROR: 值必须是浮点数\n".to_string()),
                };
                
                // 存储数据点
                db.put(ts, value)?;
                Ok("OK\n".to_string())
            },
            "GET" => {
                if parts.len() != 3 {
                    return Ok("ERROR: 格式错误，应为 GET <start_ts> <end_ts>\n".to_string());
                }
                
                let start = match parts[1].parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Ok("ERROR: 起始时间戳必须是数字\n".to_string()),
                };
                
                let end = match parts[2].parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Ok("ERROR: 结束时间戳必须是数字\n".to_string()),
                };
                
                // 查询数据
                let results = db.query(start, end)?;
                
                // 格式化结果
                let mut response = String::new();
                for (ts, val) in results {
                    response.push_str(&format!("{} {}\n", ts, val));
                }
                response.push_str("OK\n");
                Ok(response)
            },
            _ => Ok(format!("ERROR: 未知命令 '{}'\n", parts[0])),
        }
    }
}

