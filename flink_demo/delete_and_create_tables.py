import happybase

connection = happybase.Connection('localhost', 9090)

# delete all existing tables
for table in connection.tables():
    connection.delete_table(table, disable=True)
    print(f'Deleted table: {table}')

for table_name in ["TH1","TH2","HVAC1","HVAC2","MiAC1","MiAC2","MOV1","W1"]:
    connection.create_table(table_name +"_raw", {'cf':dict()})
    print(f'Created table: {table_name}_raw')

    connection.create_table(table_name +"_aggr", {'cf':dict()})
    print(f'Created table: {table_name}_aggr')

print('All done')
