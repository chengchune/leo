---
{"dg-publish":true,"permalink":"/02.项目组/2024/扬州-和熠光显-FDC/FDC自动管理程序/"}
---


#### 1 表空间管理程序
P_SOURCE_SUMMARYDATAS
```SQL
CREATE OR REPLACE PROCEDURE P_SOURCE_SUMMARYDATAS
(RETURN_VALUE OUT VARCHAR2) IS
--=================================================================================
--DESCRIPTION : 存储过程用于检查和创建表空间及分区
--=================================================================================
--DATE             NAME             VERSION             DESCRIPTION
--2024-10-07      Zhengzhong       0.1                Initial Release
--=================================================================================

--=================================================================================
--                               VARIABLE DECLARATION
--=================================================================================
   EXP_USER              EXCEPTION;
   PROCEDURE_NAME        VARCHAR2(30);
   RESULT_COUNT          VARCHAR2(20);
   DMCOUNT               NUMBER;
   MESSAGE               VARCHAR2(500);

   v_partition_name      VARCHAR2(50);         -- 分区名称
   v_tablespace_name     VARCHAR2(50);         -- 表空间名称
   v_exists_count        NUMBER;               -- 计数变量用于判断存在性
   v_partition_date      DATE;                 -- 分区截止日期
   v_datafile_path       VARCHAR2(100) := '+DATA/FDCDB/DATAFILE/'; -- 数据文件路径
   v_datafile_name       VARCHAR2(50); --数据文件名称

BEGIN

   -- SYSTEM VARIABLE INITIALIZATION -----------------------------------------------
   PROCEDURE_NAME := 'P_SOURCE_SUMMARYDATAS';
   RESULT_COUNT   := '0';
   DMCOUNT        := 0;

   -- 计算七天后的日期，并生成分区和表空间名称
   v_partition_date := TRUNC(SYSDATE + 7); -- 七天后的日期
   v_tablespace_name := 'P_DATA_TRACE_' || TO_CHAR(v_partition_date, 'YYYYMM');  -- 表空间名称
   v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYYMM');          -- 分区名称
   v_datafile_name := 'p_' || TO_CHAR(v_partition_date, 'YYYYMM');   

   -- 1. 判断表 SOURCE_SUMMARYDATAS 七天后日期对应的表空间是否存在
   SELECT COUNT(*) INTO v_exists_count
   FROM USER_TABLESPACES
   WHERE TABLESPACE_NAME = v_tablespace_name;

   IF v_exists_count = 0 THEN
      EXECUTE IMMEDIATE 'CREATE TABLESPACE ' || v_tablespace_name ||
      ' DATAFILE ''' || v_datafile_path || v_datafile_name || '_01.dbf'' SIZE 50M AUTOEXTEND ON NEXT 50M BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON';
      DMCOUNT := DMCOUNT + 1;  -- 表示表空间创建成功
      MESSAGE := '表空间 ' || v_tablespace_name || ' 已创建。';
   ELSE
      MESSAGE := '表空间 ' || v_tablespace_name || ' 已存在，跳过创建。';
   END IF;
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

    -- 2. 判断表 SOURCE_SUMMARYDATAS 七天后日期对应的分区是否存在
   SELECT COUNT(*) INTO v_exists_count
   FROM USER_TAB_PARTITIONS
   WHERE TABLE_NAME = 'SOURCE_SUMMARYDATAS_N'
   AND PARTITION_NAME = v_partition_name;

   IF v_exists_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE FDC.SOURCE_SUMMARYDATAS_N ADD PARTITION ' || v_partition_name ||
      ' VALUES LESS THAN (TO_DATE(''' ||  TO_CHAR(ADD_MONTHS(v_partition_date, 1), 'YYYYMM') || '01'', ''YYYY-MM-DD'')) TABLESPACE ' || v_tablespace_name;
      DMCOUNT := DMCOUNT + 1;  -- 表示分区创建成功
      MESSAGE := '分区 ' || v_partition_name || ' 已增加到表 SOURCE_TRACEDATAS_N。';
   ELSE
      MESSAGE := '分区 ' || v_partition_name || ' 已存在，跳过创建。';
   END IF;
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

    -- 设置返回值
   RETURN_VALUE := TO_CHAR(DMCOUNT);

   -- 完成消息
   MESSAGE := '存储过程执行完成。';
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

   COMMIT;
EXCEPTION
   WHEN EXP_USER THEN
      DBMS_OUTPUT.PUT_LINE('异常：' || MESSAGE);
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE('异常：' || SQLERRM);
END P_SOURCE_SUMMARYDATAS;
```

```SQL
CREATE OR REPLACE PROCEDURE P_SOURCE_TRACEDATAS
(RETURN_VALUE OUT VARCHAR2) IS
--=================================================================================
--DESCRIPTION : 存储过程用于检查和创建表空间及分区
--=================================================================================
--DATE             NAME             VERSION             DESCRIPTION
--2024-10-07      Zhengzhong       0.1                Initial Release
--=================================================================================

--=================================================================================
--                               VARIABLE DECLARATION
--=================================================================================
   EXP_USER              EXCEPTION;
   PROCEDURE_NAME        VARCHAR2(30);
   RESULT_COUNT          VARCHAR2(20);
   DMCOUNT               NUMBER;
   MESSAGE               VARCHAR2(500);

   v_partition_name      VARCHAR2(50);         -- 分区名称
   v_tablespace_name     VARCHAR2(50);         -- 表空间名称
   v_exists_count        NUMBER;               -- 计数变量用于判断存在性
   v_partition_date      DATE;                 -- 分区截止日期
   v_datafile_path       VARCHAR2(100) := '+DATA/FDCDB/DATAFILE/'; -- 数据文件路径
   v_datafile_name       VARCHAR2(50); --数据文件名称

BEGIN
   -- SYSTEM VARIABLE INITIALIZATION -----------------------------------------------
   PROCEDURE_NAME := 'P_SOURCE_TRACEDATAS';
   RESULT_COUNT   := '0';
   DMCOUNT        := 0;

   -- 计算七天后的日期，并生成分区和表空间名称
   v_partition_date := TRUNC(SYSDATE + 7); -- 七天后的日期
   v_tablespace_name := 'P_DATA_TRACE_' || TO_CHAR(v_partition_date, 'YYYYMM');  -- 表空间名称
   v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYYMM');          -- 分区名称
   v_datafile_name := 't_' || TO_CHAR(v_partition_date, 'YYYYMM');   

   -- 1. 判断表 SOURCE_TRACEDATAS 七天后日期对应的表空间是否存在
   SELECT COUNT(*) INTO v_exists_count
   FROM USER_TABLESPACES
   WHERE TABLESPACE_NAME = v_tablespace_name;

   IF v_exists_count = 0 THEN
      EXECUTE IMMEDIATE 'CREATE TABLESPACE ' || v_tablespace_name || ' DATAFILE ''' || v_datafile_path || v_datafile_name || '_01.dbf'' SIZE 1G AUTOEXTEND ON NEXT 1G BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON';
      DMCOUNT := DMCOUNT + 1;  -- 表示表空间创建成功
      MESSAGE := '表空间 ' || v_tablespace_name || ' 已创建。';
      DBMS_OUTPUT.PUT_LINE('CREATE TABLESPACE ' || v_tablespace_name || ' DATAFILE ''' || v_datafile_path || v_datafile_name || '_01.dbf'' SIZE 1G AUTOEXTEND ON NEXT 1G BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON');
   ELSE
      MESSAGE := '表空间 ' || v_tablespace_name || ' 已存在，跳过创建。';
   END IF;
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

   -- 2. 判断表 SOURCE_TRACEDATAS 七天后日期对应的分区是否存在
   SELECT COUNT(*) INTO v_exists_count
   FROM USER_TAB_PARTITIONS
   WHERE TABLE_NAME = 'SOURCE_TRACEDATAS_N'
   AND PARTITION_NAME = v_partition_name;

   IF v_exists_count = 0 THEN
      EXECUTE IMMEDIATE  'ALTER TABLE FDC.SOURCE_TRACEDATAS_N ADD PARTITION ' || v_partition_name || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(ADD_MONTHS(v_partition_date, 1), 'YYYYMM') || '01'', ''YYYY-MM-DD'')) TABLESPACE ' || v_tablespace_name;
      DMCOUNT := DMCOUNT + 1;  -- 表示分区创建成功
      DBMS_OUTPUT.PUT_LINE('ALTER TABLE FDC.SOURCE_TRACEDATAS_N ADD PARTITION ' || v_partition_name || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(ADD_MONTHS(v_partition_date, 1), 'YYYYMM') || '01'', ''YYYY-MM-DD'')) TABLESPACE ' || v_tablespace_name);
      MESSAGE := '分区 ' || v_partition_name || ' 已增加到表 SOURCE_TRACEDATAS_N。';
   ELSE
      MESSAGE := '分区 ' || v_partition_name || ' 已存在，跳过创建。';
   END IF;
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

   -- 设置返回值
   RETURN_VALUE := TO_CHAR(DMCOUNT);

   -- 完成消息
   MESSAGE := '存储过程执行完成。';
   DBMS_OUTPUT.PUT_LINE(MESSAGE);

   COMMIT;

EXCEPTION
   WHEN EXP_USER THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE('异常：' || MESSAGE);
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE('异常：' || SQLERRM);
END P_SOURCE_TRACEDATAS;
```
#### 2 数据文件管理程序
```SQL
CREATE OR REPLACE PROCEDURE P_TABLESPACE_AUTO_EXTEND (
    RETURN_VALUE OUT VARCHAR2
) IS
    --=================================================================================
    -- DESCRIPTION : 存储过程用于自动扩展表空间
    --=================================================================================
    -- DATE             NAME             VERSION             DESCRIPTION
    -- 2023-10-07      Zhengzhong       0.1                Initial Release
    --=================================================================================

    --=================================================================================
    --                               VARIABLE DECLARATION
    --=================================================================================
    EXP_USER              EXCEPTION;
    PROCEDURE_NAME        VARCHAR2(30) := 'P_TABLESPACE_AUTO_EXTEND';
    DMCOUNT               NUMBER := 0;
    MESSAGE               VARCHAR2(500);
    V_FILE_EXISTS         NUMBER;              -- 数据文件存在性标识
    V_FILE_NUMBER         NUMBER;              -- 数据文件计数器

    -- 表空间名称、文件名、使用率声明
    V_TABLESPACE_NAME_SUM VARCHAR2(50) := 'P_DATA_SUM_' || TO_CHAR(SYSDATE, 'YYYYMM');
    V_TABLESPACE_NAME_TRACE VARCHAR2(50) := 'P_DATA_TRACE_' || TO_CHAR(SYSDATE, 'YYYYMM');
    V_DATAFILE_NAME_SUM   VARCHAR2(100) := '+DATA/FDCDB/DATAFILE/p_' || TO_CHAR(SYSDATE, 'YYYYMM') || '_';
    V_DATAFILE_NAME_TRACE VARCHAR2(100) := '+DATA/FDCDB/DATAFILE/t_' || TO_CHAR(SYSDATE, 'YYYYMM') || '_';
    V_USED_PERCENT_SUM    NUMBER;
    V_USED_PERCENT_TRACE  NUMBER;

    -- 设定扩展阈值和初始大小
    P_THRESHOLD           CONSTANT NUMBER := 90;       -- 扩展触发的表空间使用率阈值90
    P_INITIAL_SIZE_SUM    CONSTANT VARCHAR2(20) := '50M';  -- SUM 表空间数据文件的初始大小
    P_INITIAL_SIZE_TRACE  CONSTANT VARCHAR2(20) := '1G';   -- TRACE 表空间数据文件的初始大小

    -- 动态扩展表空间的子过程
    PROCEDURE extend_tablespace (
        tablespace_name     IN VARCHAR2,
        base_datafile_name  IN VARCHAR2,
        initial_size        IN VARCHAR2,
        used_percent        IN NUMBER
    ) IS
    BEGIN
        IF used_percent > P_THRESHOLD THEN
            V_FILE_NUMBER := 1;
            LOOP
                V_FILE_NUMBER := V_FILE_NUMBER + 1;

                -- 创建数据文件名，带序号后缀
                DECLARE
                    new_datafile_name VARCHAR2(200);
                BEGIN
                    new_datafile_name := base_datafile_name || LPAD(V_FILE_NUMBER, 2, '0') || '.dbf';

                    -- 检查数据文件是否已存在
                    SELECT COUNT(*) INTO V_FILE_EXISTS 
                    FROM DBA_DATA_FILES 
                    WHERE FILE_NAME = new_datafile_name;
                    DBMS_OUTPUT.PUT_LINE('新数据文件存在性检查: ' || new_datafile_name || ' 存在数量: ' || V_FILE_EXISTS);
              
                    IF V_FILE_EXISTS = 0 THEN
                    -- 添加新的数据文件
                    EXECUTE IMMEDIATE 'ALTER TABLESPACE ' || tablespace_name || 
                                      ' ADD DATAFILE ''' || new_datafile_name || 
                                      ''' SIZE ' || initial_size || ' AUTOEXTEND ON NEXT 1G';
                                      
                    DBMS_OUTPUT.PUT_LINE('ALTER TABLESPACE ' || tablespace_name || 
                                      ' ADD DATAFILE ''' || new_datafile_name || 
                                      ''' SIZE ' || initial_size || ' AUTOEXTEND ON NEXT 1G');

                    MESSAGE := '已为表空间 ' || tablespace_name || ' 添加新的数据文件: ' || new_datafile_name;
                    DBMS_OUTPUT.PUT_LINE(MESSAGE);
                    
                    END IF;
                    
                    EXIT WHEN V_FILE_EXISTS = 0;
                END;
            END LOOP;
        ELSE
            DBMS_OUTPUT.PUT_LINE('表空间 ' || tablespace_name || ' 使用率低于 ' || P_THRESHOLD || '%，无需操作。');
        END IF;
    END extend_tablespace;

BEGIN
    -- 计算 SUM 表空间使用率
    SELECT ROUND(SUM(S.BYTES) / SUM(D.BYTES) * 100, 2) INTO V_USED_PERCENT_SUM
    FROM (SELECT TABLESPACE_NAME, SUM(BYTES) AS BYTES FROM DBA_SEGMENTS 
          WHERE TABLESPACE_NAME = V_TABLESPACE_NAME_SUM GROUP BY TABLESPACE_NAME) S,
         (SELECT TABLESPACE_NAME, COUNT(*) * 30 * 1024 * 1024 * 1024 AS BYTES FROM DBA_DATA_FILES 
          WHERE TABLESPACE_NAME = V_TABLESPACE_NAME_SUM GROUP BY TABLESPACE_NAME) D
    WHERE S.TABLESPACE_NAME = D.TABLESPACE_NAME;

    -- 计算 TRACE 表空间使用率
    SELECT ROUND(SUM(S.BYTES) / SUM(D.BYTES) * 100, 2) INTO V_USED_PERCENT_TRACE
    FROM (SELECT TABLESPACE_NAME, SUM(BYTES) AS BYTES FROM DBA_SEGMENTS 
          WHERE TABLESPACE_NAME = V_TABLESPACE_NAME_TRACE GROUP BY TABLESPACE_NAME) S,
         (SELECT TABLESPACE_NAME, COUNT(*) * 30 * 1024 * 1024 * 1024 AS BYTES FROM DBA_DATA_FILES 
          WHERE TABLESPACE_NAME = V_TABLESPACE_NAME_TRACE GROUP BY TABLESPACE_NAME) D
    WHERE S.TABLESPACE_NAME = D.TABLESPACE_NAME;
    
    DBMS_OUTPUT.PUT_LINE('SUM表空间使用率: ' || V_USED_PERCENT_SUM||'%');
    DBMS_OUTPUT.PUT_LINE('TRACE表空间使用率: ' || V_USED_PERCENT_TRACE||'%');

    -- 扩展 SUM 和 TRACE 表空间
    extend_tablespace(V_TABLESPACE_NAME_SUM, V_DATAFILE_NAME_SUM, P_INITIAL_SIZE_SUM, V_USED_PERCENT_SUM);
    extend_tablespace(V_TABLESPACE_NAME_TRACE, V_DATAFILE_NAME_TRACE, P_INITIAL_SIZE_TRACE, V_USED_PERCENT_TRACE);

    -- 设置返回值
    RETURN_VALUE := TO_CHAR(DMCOUNT);
    MESSAGE := '存储过程执行完成。';
    DBMS_OUTPUT.PUT_LINE(MESSAGE);

    COMMIT;

EXCEPTION
    WHEN EXP_USER THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('异常：' || MESSAGE);
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('异常：' || SQLERRM);
END P_TABLESPACE_AUTO_EXTEND;
```