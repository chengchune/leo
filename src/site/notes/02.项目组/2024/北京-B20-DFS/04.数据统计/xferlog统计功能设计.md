---
{"dg-publish":true,"permalink":"/02.项目组/2024/北京-B20-DFS/04.数据统计/xferlog统计功能设计/"}
---


### 1 日志文件分割程序

##### 步骤 1: 创建 `logrotate` 配置文件
在 `/etc/logrotate.d/` 目录下为 `vsftpd` 创建一个新的 `logrotate` 配置文件：
```Shell
vim /etc/logrotate.d/vsftpd
```
##### 步骤 2: 配置 `logrotate`
```Shell
/usr01/dfsadm/log/vsftpd.log /usr01/dfsadm/log/vsftpd_xferlog.log {
    daily                        # 每天轮转日志
    rotate 15
    missingok                    # 如果日志文件不存在，则忽略错误
    notifempty                   # 如果日志文件为空，则不进行轮转
    compress                     # 对日志文件进行压缩
    delaycompress                # 延迟一天压缩，确保新日志生成后再压缩
    dateext                      # 使用日期作为后缀命名
    dateformat -%Y%m%d           # 自定义日期格式（如：vsftpd.log-20241024）
    postrotate
        /bin/systemctl restart vsftpd  # 在轮转后重启 vsftpd 服务以生成新日志
    endscript
}
```
##### 步骤 3: 验证 `logrotate` 配置
```Shell
sudo logrotate -f /etc/logrotate.d/vsftpd
```
如果执行过程中没有报错，则说明配置是正确的。
##### 步骤 4. 设置 `crontab` 定时任务
`sudo crontab -e`
在其中添加以下行，每天凌晨 0:00 自动执行日志轮转：
`58 23 * * * /usr/sbin/logrotate /etc/logrotate.d/vsftpd`

### 2 xferlog 解析并写入Oracle表
##### 2.1 xferlog 表结构
```SQL

-- 创建序列，用于生成日志记录的唯一 ID 
CREATE SEQUENCE SEQ_DFS_FTP_UPLOAD_LOG START WITH 1 -- 序列从1开始 
INCREMENT BY 1 -- 每次递增1 
NOCACHE -- 不缓存序列值 
NOCYCLE; -- 序列值不会循环 

-- 创建表用于存储 FTP 上传日志
CREATE TABLE dfs_ftp_upload_log (
    id            NUMBER(10) PRIMARY KEY,  
    event_time    TIMESTAMP,              
    transfer_type VARCHAR2(1),           
    direction     VARCHAR2(1),            
    access_mode   VARCHAR2(1),             
    username      VARCHAR2(100),           
    filesize      NUMBER,                  
    filepath      VARCHAR2(1024),          
    ip_address    VARCHAR2(45),           
    status        VARCHAR2(1),            
    xferlog_time  TIMESTAMP                
);

COMMENT ON TABLE dfs_ftp_upload_log IS 'DFS FTP上传日志记录表';
COMMENT ON COLUMN dfs_ftp_upload_log.id IS '日志ID，主键';
COMMENT ON COLUMN dfs_ftp_upload_log.event_time IS '事件发生的具体时间';
COMMENT ON COLUMN dfs_ftp_upload_log.transfer_type IS '传输类型，b 表示二进制传输，a 表示ASCII传输';
COMMENT ON COLUMN dfs_ftp_upload_log.direction IS '传输方向，i 表示上传，o 表示下载';
COMMENT ON COLUMN dfs_ftp_upload_log.access_mode IS '访问模式，r 表示读取文件（下载），w 表示写入文件（上传）';
COMMENT ON COLUMN dfs_ftp_upload_log.username IS '操作FTP的用户名';
COMMENT ON COLUMN dfs_ftp_upload_log.filesize IS '文件的大小，单位是字节';
COMMENT ON COLUMN dfs_ftp_upload_log.filepath IS '传输的文件在服务器上的完整路径';
COMMENT ON COLUMN dfs_ftp_upload_log.ip_address IS '客户端的IP地址，IPv4或者IPv6格式';
COMMENT ON COLUMN dfs_ftp_upload_log.status IS '文件传输的状态，c 表示传输完成，i 表示传输中断';
COMMENT ON COLUMN dfs_ftp_upload_log.xferlog_time IS 'xferlog文件生成的时间';
```

##### 2.2 日志解析程序 

> parse_xferlog_to_oracle.sh
```Shell 
#!/bin/bash
# 设置 NLS_LANG 环境变量
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8

# 设置终端编码
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# 配置参数
LOG_DIR="/var/log/"            # 日志目录，可配置
OK_DIR="${LOG_DIR}/OK"         # 已处理日志目录
ORACLE_CONN_STR="dfs/dfs@172.16.30.8:1521/glorydb"

# 创建 OK 目录
if [ ! -d "$OK_DIR" ]; then
    mkdir -p "$OK_DIR"
fi

# 获取当前日期的前一天
yesterday=$(date -d "yesterday" +"%Y%m%d")
echo "昨天的日期: $yesterday"
echo "日志目录: $LOG_DIR"

# 查找日志目录中包含 xferlog 和 yyyymmdd 日期的日志文件（不递归）
logfiles=$(find "$LOG_DIR" -maxdepth 1 -type f -name "*xferlog*$yesterday*")
echo "找到的日志文件: $logfiles"

# 确保找到的文件不为空
if [ -z "$logfiles" ]; then
    echo "没有找到符合条件的日志文件。"
    exit 1
fi

# 使用 for 循环逐个处理找到的文件
for logfile in $logfiles; do
    echo "处理日志文件: $logfile"

    # 检查日志文件是否为空
    if [ ! -s "$logfile" ]; then
        echo "日志文件为空，跳过: $logfile"
        continue
    fi

    # 读取并解析日志文件
    echo "开始读取日志文件内容..."
    while IFS= read -r line || [[ -n $line ]]; do
        # 确保能成功读取行
        if [ -z "$line" ]; then
            echo "读取到空行，跳过"
            continue
        fi

        # 输出读取的每一行
        echo "读取行: $line"

        # 解析日志行的各个字段
        event_time_raw=$(echo "$line" | awk '{print $1" "$2" "$3" "$4" "$5}')
        event_time=$(date -d "$event_time_raw" +"%Y-%m-%d %H:%M:%S")  # 转换为 yyyy-MM-dd HH:mm:ss 格式
        ip_address=$(echo "$line" | awk '{print $7}' | sed 's/::ffff://g')  # 移除 ::ffff: 前缀
        filesize=$(echo "$line" | awk '{print $8}')
        # 正确提取 filepath
        filepath=$(echo "$line" | awk '{print $9}')  
        # 将 filepath 转换为 UTF-8
        filepath=$(echo -e "$filepath" | iconv -f GBK -t UTF-8)       

        transfer_type=$(echo "$line" | awk '{print $10}')
        direction=$(echo "$line" | awk '{print $12}')
        access_mode=$(echo "$line" | awk '{print $13}')
        username=$(echo "$line" | awk '{print $14}')
        status=$(echo "$line" | awk '{print $18}')  # 修改为正确的索引

        # 将路径进行解码，确保特殊字符被正确处理
        decoded_filepath=$(echo -e "$filepath" | iconv -f ISO-8859-1 -t UTF-8//IGNORE)  # 假设原始编码为 ISO-8859-1

        # 输出生成的 SQL 语句
        echo "准备插入数据: event_time=$event_time, transfer_type=$transfer_type, direction=$direction, access_mode=$access_mode, username=$username, filesize
=$filesize, filepath=$filepath, ip_address=$ip_address, status=$status"

        # 将数据插入到 Oracle 数据库并捕获输出
        sql_output=$(sqlplus -s "$ORACLE_CONN_STR" <<EOF
          SET HEADING OFF
          SET FEEDBACK OFF
          INSERT INTO dfs_ftp_upload_log (id, event_time, transfer_type, direction, access_mode, username, filesize, filepath, ip_address, status, xferlog_tim
e) 
          VALUES (SEQ_DFS_FTP_UPLOAD_LOG.NEXTVAL, TO_TIMESTAMP('$event_time', 'YYYY-MM-DD HH24:MI:SS'), '$transfer_type', '$direction', '$access_mode', '$user
name', $filesize, '$filepath', '$ip_address', '$status', SYSTIMESTAMP);
          COMMIT;
EOF
        )

        # 检查 SQL*Plus 的退出状态
        if [ $? -ne 0 ]; then
            echo "数据库操作失败，输出: $sql_output"
        else
            echo "数据库操作成功"
        fi
        
        # 输出 SQL 的结果
        echo "$sql_output"
    done < "$logfile"  # 将日志文件传递给 while 循环的标准输入

    # 将解析完成的日志文件移动到 OK 文件夹
    mv "$logfile" "$OK_DIR/"
    echo "已将处理完成的日志文件移动到: $OK_DIR/"
done
```

定时任务: 
20 0 * * * /path/to/your/parse_xferlog_to_oracle.sh >> /path/to/log/parse_xferlog_to_oracle.sh.log 2>&1
### 3 vsftp 解析并写入oracle表
##### 3.1 vsftpd表结构
```SQL
-- 创建表用于存储 FTP 删除日志 
CREATE TABLE dfs_ftp_delete_log 
( id NUMBER PRIMARY KEY, -- 日志记录的唯一 ID 
 event_time TIMESTAMP, -- 事件发生时间，格式为 yyyy-mm-dd hh24:mi:ss 
 ip_address VARCHAR2(45), -- IP 地址 
 username VARCHAR2(50), -- 执行操作的用户名 
 file_name VARCHAR2(255), -- 被删除的文件名称 
 delete_status VARCHAR2(50), -- 删除状态 
 xferlog_time TIMESTAMP DEFAULT 
 SYSTIMESTAMP -- 记录插入时间，默认为当前时间 ); 
 
 -- 创建序列，用于生成日志记录的唯一 ID 
CREATE SEQUENCE SEQ_DFS_FTP_DELETE_LOG START WITH 1 -- 序列从1开始 
 INCREMENT BY 1 -- 每次递增1 
 NOCACHE -- 不缓存序列值 
 NOCYCLE; -- 序列值不会循环
 
 -- 创建索引（可选），根据需求可以对特定字段进行索引优化 
 CREATE INDEX idx_event_time ON dfs_ftp_delete_log(event_time); 
 CREATE INDEX idx_username ON dfs_ftp_delete_log(username);
```
##### 3.2 日志解析程序
> parse_vsftpd_to_oracle.sh
```Shell
#!/bin/bash

# 设置 NLS_LANG 环境变量
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8

# 设置终端编码
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# 配置参数
ORACLE_CONN_STR="dfs/dfs@172.16.30.8:1521/glorydb"
LOG_DIR="/var/log"  # 修改为实际的日志目录
OK_DIR="$LOG_DIR/OK"     # 存放解析完成日志的目录

# 如果 OK 文件夹不存在，则创建它
mkdir -p "$OK_DIR"

# 获取前一天的日期
yesterday_date=$(date -d 'yesterday' +%Y%m%d)
echo "处理日期：$yesterday_date"
echo "日志目录：$LOG_DIR"

# 查找日志目录中包含 vsftpd 和前一天日期的日志文件
log_files=$(find "$LOG_DIR" -maxdepth 1 -type f -name "*vsftpd*$yesterday_date*")
echo "找到的日志文件：$log_files"

# 确保找到的文件不为空
if [ -z "$log_files" ]; then
    echo "没有找到符合条件的日志文件。"
    exit 1
fi

# 遍历找到的日志文件
for log_file in $log_files; do
    echo "处理日志文件：$log_file"

    # 从日志文件中提取 "OK DELETE" 操作的行
    grep "OK DELETE" "$log_file" | while read -r line; do
        echo "解析行：$line"

        # 提取并格式化事件时间
        event_time=$(echo "$line" | awk '{print $1" "$2" "$3" "$4}')
        formatted_time=$(date -d "$event_time" '+%Y-%m-%d %H:%M:%S')
        echo "事件时间：$formatted_time"

        # 提取 IP 地址
        ip_address=$(echo "$line" | grep -oP '::ffff:\K[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "$line" | grep -oP '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
        echo "IP 地址：$ip_address"

        # 提取用户名
        username=$(echo "$line" | awk -F'[][]' '{print $4}')
        echo "用户名：$username"

        # 提取文件名并去除多余空格
       file_name=$(echo "$line" | awk -F'OK DELETE:' '{print $2}' | sed -E 's/^ *(Client "::ffff:[^"]+", *")?//; s/"$//')
       file_name=$(echo -e "$file_name" | iconv -f GBK -t UTF-8)
        echo "文件名：$file_name"

        # 将解析后的数据写入 Oracle 数据库
        sql_output=$(sqlplus -s "$ORACLE_CONN_STR" <<EOF
          SET HEADING OFF
          SET FEEDBACK OFF
          WHENEVER SQLERROR EXIT SQL.SQLCODE
          INSERT INTO dfs_ftp_delete_log (id, event_time, ip_address, username, file_name, delete_status)
          VALUES (SEQ_DFS_FTP_DELETE_LOG.NEXTVAL, TO_TIMESTAMP('$formatted_time', 'YYYY-MM-DD HH24:MI:SS'), '$ip_address', '$username', '$file_name', 'Y');
          COMMIT;
EOF
        )

        # 检查 SQL*Plus 的退出状态
        if [ $? -ne 0 ]; then
            echo "数据库操作失败，输出：$sql_output"
        else
            echo "数据库操作成功"
        fi
    done

    # 将解析完成的日志文件移动到 OK 文件夹
    mv "$log_file" "$OK_DIR/"
    echo "已将处理完成的日志文件移动到：$OK_DIR"
done
```

定时任务:
20 0 * * * /path/to/your/parse_xferlog_to_oracle.sh >> /path/to/log/parse_xferlog_to_oracle.sh.log 2>&1
### 4 日志清理程序
##### 4.1 定期清理15天前日志文件   
> clear_vsftp_log.sh
```Shell
#!/bin/bash

# 清理过期日志的脚本
# 默认日志目录
LOG_DIR="/usr01/dfsadm/log"
# 日志保留的天数
RETAIN_DAYS=15

# 检查是否传入了自定义日志目录
if [ ! -z "$1" ]; then
  LOG_DIR=$1
fi

# 检查日志目录是否存在
if [ ! -d "$LOG_DIR" ]; then
  echo "日志目录不存在: $LOG_DIR"
  exit 1
fi

echo "开始清理 $LOG_DIR 目录下超过 $RETAIN_DAYS 天的日志文件..."

# 查找并删除超过指定天数的 .log 或 .log-日期格式的文件
find "$LOG_DIR" -type f -mtime +$RETAIN_DAYS -regex ".*\.log\(-[0-9]+\)*" -exec rm -f {} \;

# 删除空目录
find "$LOG_DIR" -type d -empty -delete

# 反馈删除操作结果
if [ $? -eq 0 ]; then
  echo "清理完成: 已删除 $LOG_DIR 目录下超过 $RETAIN_DAYS 天的日志文件。"
else
  echo "清理过程中出现错误，请检查。"
fi

exit 0

```
--定期每天03:00 执行一次
```Shell
chmod +x /path/to/clear_vsftp_log.sh
0 3 * * * /path/to/clear_vsftp_log.sh
```

三台服务器部署，配置 每天03:00 执行一次
### 5 oracle client安装
``` Shell
# 1 安装Oracle客户端软件
rpm -ivh /Glorysoft/etl/app/oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm 
rpm -ivh /Glorysoft/etl/app/oracle-instantclient11.2-devel-11.2.0.4.0-1.x86_64.rpm 
rpm -ivh /Glorysoft/etl/app/oracle-instantclient11.2-sqlplus-11.2.0.4.0-1.x86_64.rpm  

# 2 修改环境变量   
vi /etc/profile  
  
export  ORACLE_HOME=/usr/lib/oracle/11.2/client64  
export  TNS_ADMIN=$ORACLE_HOME/network/admin  
export  NLS_LANG='simplified chinese_china'.ZHS16GBK  
export  LD_LIBRARY_PATH=$ORACLE_HOME/lib  
export  PATH=$ORACLE_HOME/bin:$PATH  
  
最后执行  
source /etc/profile  
sqlplus 能够出现命令，即安装成功
```

#### 6 配置表
##### 6.1 用户配置表
```SQL
CREATE TABLE dfs_user_config ( 
id NUMBER(10) PRIMARY KEY, -- 用户ID，主键 
user_name VARCHAR2(100) NOT NULL, -- 用户名 
local_path VARCHAR2(255), -- 用户本地路径 
is_user NUMBER(1) DEFAULT 1, -- 用户状态，1为启用，0为禁用 
create_time TIMESTAMP DEFAULT SYSTIMESTAMP -- 创建时间，默认当前时间 ); 
-- 添加注释 
COMMENT ON TABLE dfs_user_config IS '用户配置信息表'; 
COMMENT ON COLUMN dfs_user_config.id IS '用户ID，主键'; 
COMMENT ON COLUMN dfs_user_config.user_name IS '用户名'; 
COMMENT ON COLUMN dfs_user_config.local_path IS '用户本地路径'; 
COMMENT ON COLUMN dfs_user_config.is_user IS '用户状态，1为启用，0为禁用'; 
COMMENT ON COLUMN dfs_user_config.create_time IS '创建时间';
```

##### 6.2 设备配置表
```SQL
CREATE TABLE dfs_eqp_config ( 
id NUMBER(10) PRIMARY KEY, -- 设备ID，主键 
eqp_name VARCHAR2(100) NOT NULL, -- 设备名称 
user_name VARCHAR2(100) NOT NULL, -- 用户名，关联到 dfs_user_config 表中的 user_name 
ip_address VARCHAR2(45), -- 设备IP地址 
is_use NUMBER(1) DEFAULT 1, -- 设备使用状态，1为启用，0为禁用 
create_time TIMESTAMP DEFAULT SYSTIMESTAMP -- 创建时间，默认当前时间 ); 
-- 添加注释 
COMMENT ON TABLE dfs_eqp_config IS '设备配置信息表'; 
COMMENT ON COLUMN dfs_eqp_config.id IS '设备ID，主键'; 
COMMENT ON COLUMN dfs_eqp_config.eqp_name IS '设备名称'; 
COMMENT ON COLUMN dfs_eqp_config.user_name IS '用户名，关联到 dfs_user_config 表'; COMMENT ON COLUMN dfs_eqp_config.ip_address IS '设备IP地址'; 
COMMENT ON COLUMN dfs_eqp_config.is_use IS '设备使用状态，1为启用，0为禁用'; 
COMMENT ON COLUMN dfs_eqp_config.create_time IS '创建时间';
```

### 7 业务计算逻辑
###### 7.1 按天汇总逻辑

###### 7.2 清除30天之前明细数据逻辑


###### 7.3 其他逻辑
