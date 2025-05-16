export RUST_LOG=info
cargo build --release
cargo run

# 将所有日志输出重定向到文件
# RUST_LOG=info cargo run >tsdb.log 2>&1

# 设置更详细的日志级别
# RUST_LOG=debug cargo run >tsdb_debug.log 2>&1
