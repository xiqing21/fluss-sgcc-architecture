-- 创建Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- 查看所有已创建的表
SHOW TABLES;
