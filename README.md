## 可视化数据

访问 Neo4j 浏览器来可视化你的图数据：
- 本地地址：http://localhost:7474
- 使用相同的用户名和密码登录


### 有用的Cypher查询
```cypher
// 查看所有点
MATCH (n) RETURN n

// 查看数据结构
CALL db.schema.visualization()

//查找所有点和关系
MATCH p=()-->() RETURN p;
MATCH (n)-[r]->(m) RETURN n, r, m
 
// 查找路径
MATCH p = (a:Person)-[*1..3]-(b:Person) 
WHERE a.name = '张三' AND b.name = '王五'
RETURN p
```
