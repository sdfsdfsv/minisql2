from flask import Flask, request, jsonify
from Api import Api, Table, Attribute, TableRow, Condition

app = Flask(__name__)
api = Api()

@app.route('/create_table', methods=['POST'])
def create_table():
    table_name = request.json['table_name']
    primary_key = request.json['primary_key']
    attributes = []
    for attr in request.json['attributes']:
        attributes.append(Attribute(attr['name'], attr['type'], attr['size'], attr['nullable']))
    table = Table(table_name, primary_key, attributes)
    api.createTable(table)
    return jsonify({'message': f'Table {table_name} created successfully'})


@app.route('/insert_row', methods=['POST'])
def insert_row():
    table_name = request.json['table_name']
    values = []
    for value in request.json['values']:
        values.append(value)
    row = TableRow(values)
    api.insertRow(table_name, row)
    return jsonify({'message': 'Row inserted successfully'})

@app.route('/select', methods=['POST'])
def select():
    table_name = request.json['table_name']
    columns = request.json['columns']
    conditions = []
    for cond in request.json['conditions']:
        conditions.append(Condition(cond['column'], cond['operator'], cond['value']))
    result = api.select(table_name, columns, conditions)

    return jsonify({'rows': [str(r) for r in result]})


@app.route('/delete_row', methods=['POST'])
def delete_row():
    table_name = request.json['table_name']
    conditions = []
    for cond in request.json['conditions']:
        conditions.append(Condition(cond['column'], cond['operator'], cond['value']))
    api.deleteRow(table_name, conditions)
    return jsonify({'message': 'Row deleted successfully'})


@app.route('/drop_table', methods=['POST'])
def drop_table():
    table_name = request.json['table_name']   
    api.dropTable(table_name)
    return jsonify({'message': f'Table {table_name}  dropped successfully'})



if __name__ == '__main__':
    app.run(debug=True)
