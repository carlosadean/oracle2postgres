def oracle2postgres(oraclecolumns):
    """ Mapping Oracle data type to Postgres."""
    
    pgcols = ""

    for column in oraclecolumns:
        if column[1] == 'NUMBER' and (column[3]) >= 10 and column[4] == 0:
            pgcols += " %s %s," %(column[0],'bigint')
        elif column[1] == 'NUMBER' and column[3] >= 5 and column[3] <= 9 and column[4] == 0:
            pgcols += " %s %s," %(column[0],'integer')
        elif column[1] == 'NUMBER' and column[3] >= 1 and column[3] <= 4 and column[4] == 0:
            pgcols += " %s %s," %(column[0],'smallint')
        elif column[1] == 'NUMBER' and column[3] >= 1 and column[3] <= 6 and column[4] >= 1:
            pgcols += " %s %s," %(column[0],'real')
        elif column[1] == 'BINARY_DOUBLE' and column[3] >= 1:
            pgcols += " %s %s," %(column[0],'double precision')
        elif column[1] == 'NUMBER' and column[3] >= 7 and column[4] >= 1:
            pgcols += " %s %s," %(column[0],'double precision')
        elif column[1] == 'BINARY_FLOAT' and column[2] >= 1 and column[3] == 'Null' and column[4] == 'Null':
            pgcols += " %s %s," %(column[0],'double precision')
        elif column[1] == 'VARCHAR2':
            pgcols += " %s %s," %(column[0],'text')
        else:
            print column
            print '%s is a unknown data type (%s) for this function' %(column[1],column[4])

    pgcols = pgcols.replace(' ',' ')[:-1]

    return pgcols

def get_columns_and_datatypes(self, tablename):
""" Get columns and data types from Oracle database"""

sql = """SELECT column_name, data_type, data_length,
        data_precision, data_scale FROM all_tab_columns
        WHERE table_name = '%s' """ %(tablename.upper())

try:
    oraclecolumns = self.execute(sql)
    pg_columns = dbutils.oracle2postgres(oraclecolumns)
    print pg_columns
except Excetion, error:
    print error
    return error

return pg_columns
