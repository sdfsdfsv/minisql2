import requests
import json

# 定义 API 端点
CREATE_TABLE_ENDPOINT = 'http://localhost:5000/create_table'
INSERT_ROW_ENDPOINT = 'http://localhost:5000/insert_row'
SELECT_ENDPOINT = 'http://localhost:5000/select'
DELETE_ROW_ENDPOINT = 'http://localhost:5000/delete_row'

# 定义表格和行数据
table_name = 'students'
primary_key = 'id'
attributes = [
    {'name': 'id', 'type': 'INT', 'size': 4, 'nullable': True},
    {'name': 'name', 'type': 'CHAR', 'size': 12, 'nullable': False},
    {'name': 'category', 'type': 'CHAR', 'size': 20, 'nullable': False}
]
rows = [
    {'id': 0, 'name': 'ljx0', 'category': '0man'},
    {'id': 1, 'name': 'ljx1', 'category': '1man'},
]

# 创建表格
table_data = {
    'table_name': table_name,
    'primary_key': primary_key,
    'attributes': attributes
}
response = requests.post(CREATE_TABLE_ENDPOINT, json=table_data)
print(response.json())

# 插入行数据
for row in rows:
    row_data = {
        'table_name': table_name,
        'values': [row['id'], row['name'], row['category']]
    }
    response = requests.post(INSERT_ROW_ENDPOINT, json=row_data)
    print(response.json())

# 查询数据
select_data = {
    'table_name': table_name,
    'columns': ['id', 'name', 'category'],
    'conditions': [{'column': 'id', 'operator': '=', 'value': '1'}]
}
response = requests.post(SELECT_ENDPOINT, json=select_data)
print(response.json())

# 删除行数据
delete_data = {
    'table_name': table_name,
    'conditions': [{'column': 'id', 'operator': '!=', 'value': '0'}]
}
response = requests.post(DELETE_ROW_ENDPOINT, json=delete_data)
print(response.json())

# 查询数据
select_data = {
    'table_name': table_name,
    'columns': ['id', 'name', 'category'],
    'conditions': []
}
response = requests.post(SELECT_ENDPOINT, json=select_data)
print(response.json())
