# 匿名活动明细与组织汇总的本地仓湖

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

所有示例标识均为本地合成，不包含真实人员信息。来源与别名的组合唯一映射到匿名参与者，当前不允许把既有别名静默改给另一人。原始区保留报送原文，DWD活动事实保留规范化身份及原始标识。一个Parquet可能包含多人，版本固定明细文件与组织级汇总。个人导出是明确目标文件；仓内受管理目录为landing、facts与catalog.sqlite3，任意复制到仓外的文件不在本地仓的可擦除边界内。

## 代码职责

- `privacywarehouse/subjects.py`：来源别名与匿名身份映射。
- `privacywarehouse/landing.py`：原始接收和身份规范化。
- `privacywarehouse/warehouse.py`：列式明细、组织聚合和个人导出。
- `privacywarehouse/storage.py`：本地事务、内容摘要和查询工具。

## 安装与运行

使用 Python 3.11（兼容该系列补丁版本，独立容器镜像以 SHA256 摘要固定）。构建工具和运行依赖均按精确版本登记。先执行 `python3 -m pip install -r requirements-build.txt -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q privacywarehouse tests
python3 -m build --wheel --no-isolation
python3 -m unittest discover -s tests -v
python3 -m privacywarehouse
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m privacywarehouse --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t warehouse-erasure:local .
docker run --rm --network none warehouse-erasure:local
docker run --rm --network none warehouse-erasure:local python3 -m privacywarehouse
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
