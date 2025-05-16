use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use log::{info, error, debug};

use crate::db::SimpleTSDB;
use crate::error::Result;
use crate::types::{DataPoint, QueryFilter};

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
                // 兼容旧格式: PUT <timestamp> <value>
                if parts.len() == 3 {
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
                    return Ok("OK\n".to_string());
                } else {
                    return Ok("ERROR: 格式错误，应为 PUT <timestamp> <value>\n".to_string());
                }
            },
            
            "INSERT" => {
                // 新格式: INSERT <measurement>[,tag1=val1,tag2=val2...] field1=val1,field2=val2... <timestamp>
                if parts.len() != 4 {
                    return Ok("ERROR: 格式错误，应为 INSERT measurement,tags fields timestamp\n".to_string());
                }
                
                // 解析测量名称和标签
                let measurement_tags = parts[1];
                let measurement; // 移除mut关键字
                let mut tags = std::collections::HashMap::new();
                
                let mt_parts: Vec<&str> = measurement_tags.split(',').collect();
                if mt_parts.is_empty() {
                    return Ok("ERROR: 缺少测量名称\n".to_string());
                }
                
                measurement = mt_parts[0];
                for i in 1..mt_parts.len() {
                    let kv: Vec<&str> = mt_parts[i].split('=').collect();
                    if kv.len() == 2 {
                        tags.insert(kv[0].to_string(), kv[1].to_string());
                    }
                }
                
                // 解析字段
                let fields_str = parts[2];
                let mut fields = std::collections::HashMap::new();
                
                let fields_parts: Vec<&str> = fields_str.split(',').collect();
                for part in fields_parts {
                    let kv: Vec<&str> = part.split('=').collect();
                    if kv.len() == 2 {
                        if let Ok(value) = kv[1].parse::<f64>() {
                            fields.insert(kv[0].to_string(), value);
                        } else {
                            return Ok(format!("ERROR: 字段值必须是数字: {}\n", kv[1]));
                        }
                    }
                }
                
                // 解析时间戳
                let timestamp = match parts[3].parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Ok("ERROR: 时间戳必须是整数\n".to_string()),
                };
                
                // 创建数据点
                let mut point = DataPoint::new(timestamp);
                for (k, v) in tags {
                    point.add_tag(k, v);
                }
                for (k, v) in fields {
                    point.add_field(k, v);
                }
                
                // 写入数据库
                db.write_point(measurement, point)?;
                return Ok("OK\n".to_string());
            },
            
            "GET" => {
                // 兼容旧格式: GET <start_ts> <end_ts>
                if parts.len() == 3 {
                    let start = match parts[1].parse::<u64>() {
                        Ok(ts) => ts,
                        Err(_) => return Ok("ERROR: 起始时间戳必须是数字\n".to_string()),
                    };
                    
                    let end = match parts[2].parse::<u64>() {
                        Ok(ts) => ts,
                        Err(_) => return Ok("ERROR: 结束时间戳必须是数字\n".to_string()),
                    };
                    
                    // 查询数据
                    let results = db.legacy_query(start, end)?;
                    
                    // 格式化结果
                    let mut response = String::new();
                    for (ts, val) in results {
                        response.push_str(&format!("{} {}\n", ts, val));
                    }
                    response.push_str("OK\n");
                    return Ok(response);
                } else {
                    return Ok("ERROR: 格式错误，应为 GET <start_ts> <end_ts>\n".to_string());
                }
            },
            
            "QUERY" => {
                // 高级查询格式: QUERY measurement[,tag1=val1] field1,field2 start_ts end_ts
                if parts.len() < 4 {
                    return Ok("ERROR: 格式错误，应为 QUERY measurement,tags fields start_ts end_ts\n".to_string());
                }
                
                // 解析查询参数
                let measurement_tags = parts[1];
                let fields_str = parts[2];
                
                let start_ts = match parts[3].parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Ok("ERROR: 起始时间戳必须是整数\n".to_string()),
                };
                
                let end_ts = if parts.len() > 4 {
                    match parts[4].parse::<u64>() {
                        Ok(ts) => ts,
                        Err(_) => return Ok("ERROR: 结束时间戳必须是整数\n".to_string()),
                    }
                } else {
                    u64::MAX
                };
                
                // 创建查询过滤器
                let mut filter = QueryFilter::new(start_ts, end_ts);
                
                // 解析测量名称和标签
                let mt_parts: Vec<&str> = measurement_tags.split(',').collect();
                if !mt_parts.is_empty() {
                    filter = filter.measurement(mt_parts[0]);
                    
                    for i in 1..mt_parts.len() {
                        let kv: Vec<&str> = mt_parts[i].split('=').collect();
                        if kv.len() == 2 {
                            filter = filter.add_tag(kv[0], kv[1]);
                        }
                    }
                }
                
                // 解析字段
                if fields_str != "*" {
                    let fields_parts: Vec<&str> = fields_str.split(',').collect();
                    for field in fields_parts {
                        filter = filter.add_field(field);
                    }
                }
                
                // 执行查询
                let results = db.query(filter)?;
                
                // 格式化查询结果
                let mut response = String::new();
                for (series_key, fields) in results {
                    response.push_str(&format!("# 序列: {}{}\n", 
                        series_key.measurement,
                        {
                            let mut tags_str = String::new();
                            for (k, v) in &series_key.tags {
                                tags_str.push_str(&format!(",{}={}", k, v));
                            }
                            tags_str
                        }
                    ));
                    
                    for (field_name, points) in fields {
                        response.push_str(&format!("## 字段: {}\n", field_name));
                        for (ts, val) in points {
                            response.push_str(&format!("{} {}\n", ts, val));
                        }
                    }
                    response.push('\n');
                }
                response.push_str("OK\n");
                return Ok(response);
            },
            
            _ => Ok(format!("ERROR: 未知命令 '{}'\n", parts[0])),
        }
    }
}

