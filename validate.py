import snowflake.connector
import csv

# Gets the version
ctx = snowflake.connector.connect(
    user='databar',
    password='Databar1!',
    account='mta30327.us-east-1'
)
cs = ctx.cursor()
cs2 = ctx.cursor()
cs3 = ctx.cursor()
result = cs.execute('show databases')
print("printing result")
databases = []
for row in result:
    # print('row:' + str(row))
    # print('\ndatabase:' + row[1])
    databases.append(row[1])
row_list = []
print(databases)
for i in databases:
    sql = 'show schemas in ' + i
    # schemas:
    result2 = cs.execute(sql)
    for row2 in result2:
        sql3 = 'show tables in ' + i + '.' + row2[1]
        # tables
        result3 = cs2.execute(sql3)
        for row3 in result3:
            sql4 = 'desc table ' + i + '.' + row2[1] + '.' + row3[1];
            result4 = cs3.execute(sql4)
            # columns and data types
            for row4 in result4:
                row_list.append([i, row2[1], row3[1], row4[0], row4[1]])

f = open("tt.csv", "w")
f.write("Database, Schema, TableName/ViewName, ColumnName, DataType" + "\n")
for i in range(len(row_list)):
    arr_to_string = ','.join(map(str, row_list[i]))
    f.write(str(arr_to_string) + "\n")

f.close()