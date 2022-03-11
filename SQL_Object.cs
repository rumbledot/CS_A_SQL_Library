using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using A_Common_Library.Data;
using A_SQL_Library.Helpers;

namespace DB_SQL_Library
{
    public class SQL_Object<T>
    {
        private string connection_string;
        private string table_name;
        public string pk_column_name { get; private set; }
        public bool pk_auto_increment { get; private set; }
        public bool use_pk { get { return string.IsNullOrEmpty(pk_column_name); } }
        public DataTable table_dt { get; private set; }
        public string storedproc_prefix { get; set; }

        private List<PropertyInfo> Invoice_properties = new List<PropertyInfo>();
        private List<PropertyInfo> InvoiceLine_properties = new List<PropertyInfo>();
        private Dictionary<string, Type> datatype_mapper = new Dictionary<string, Type>();

        public SQL_Object(string table_name, string connection_string)
        {
            if (string.IsNullOrEmpty(table_name)) throw new Exception("Please provide Database table's name");
            if (string.IsNullOrEmpty(connection_string)) throw new Exception("Please provide Database connection credentials");

            this.table_name = table_name;
            this.connection_string = connection_string;
            this.pk_column_name = string.Empty;
            this.storedproc_prefix = "";

            datatype_mapper = new Dictionary<string, Type>()
            {
                { "varchar", typeof(string) },
                { "smallint", typeof(Int16) },
                { "int", typeof(int) },
                { "bigint", typeof(long) },
                { "float", typeof(float) },
                { "decimal", typeof(decimal) },
                { "bit", typeof(bool) },
                { "datetime", typeof(DateTime) }
            };

            this.Build_AutoDatatable();
            this.Get_Table_PK();
        }

        private void Build_Datatable()
        {
            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataTable result = new DataTable();

                    connection.Open();

                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = $"SELECT DISTINCT " +
                        "c.COLUMN_NAME " +
                        ",c.IS_NULLABLE " +
                        ",c.DATA_TYPE " +
                        ",c.CHARACTER_MAXIMUM_LENGTH " +
                        ",c.NUMERIC_PRECISION " +
                        ",c.NUMERIC_SCALE " +
                        ",sc.is_identity " +
                        "FROM INFORMATION_SCHEMA.COLUMNS c " +
                        "INNER JOIN sys.columns sc ON sc.name = c.COLUMN_NAME " +
                        $"WHERE c.TABLE_NAME=@TABLE_NAME";
                    command.Parameters.Add(new SqlParameter("@TABLE_NAME", this.table_name));
                    command.ExecuteNonQuery();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);

                    adapter.Fill(result);

                    if (!result.NotEmpty()) throw new Exception("Table not initialised");

                    this.table_dt = new DataTable();

                    foreach (DataRow row in result.Rows)
                    {
                        this.table_dt.NewColumn(row.Field<string>("COLUMN_NAME"), this.datatype_mapper[row.Field<string>("DATA_TYPE")], row.Value<bool>("IS_NULLABLE"));
                    }

                    return;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        private void Build_AutoDatatable() 
        {
            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataSet result = new DataSet();

                    connection.Open();
                    
                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = $"SELECT * FROM {this.table_name} WHERE 1=0;" +
                        $"SELECT " +
                        "     KU.table_name as TABLENAME " +
                        "    ,column_name as PRIMARYKEYCOLUMN " +
                        "	,SC.is_identity AS INCREMENTAL " +
                        "	,SC.is_nullable AS NULLABLE " +
                        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
                        "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
                        "    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                        "    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
                        $"    AND KU.table_name = '{this.table_name}' " +
                        "INNER JOIN sys.columns SC ON SC.name = COLUMN_NAME AND SC.is_nullable = 0 AND is_identity = 1 " +
                        "ORDER BY " +
                        "     KU.TABLE_NAME " +
                        "    ,KU.ORDINAL_POSITION;";
                    command.ExecuteNonQuery();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);

                    adapter.Fill(result);

                    if (result != null
                        && result.Tables.Count == 2) 
                    {
                        this.table_dt = result.Tables[0].Copy();
                        this.pk_column_name = result.Tables[1].Rows[0].Value<string>("TABLENAME");
                        this.pk_auto_increment = result.Tables[1].Rows[0].Value<bool>("INCREMENTAL");

                        return;
                    }

                    throw new Exception("Table not initialised");
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally 
                {
                    connection.Close();
                }
            }
        }

        public void CreateStoredProcFile() 
        {
            string stored_proc = "", columns = "";
            stored_proc = $"CREATE PROCEDURE sp_{this.storedproc_prefix}Save_{this.table_name}\n";
            stored_proc += "(\n";

            stored_proc += StoredProcBuilder.Build_InsertValueList(this.table_dt);

            stored_proc += ")\nAS\nBEGIN\n";
            stored_proc += $"IF (SELECT COUNT(*) FROM tblInvoice WHERE {this.pk_column_name}=@Document_ID) > 0\n";
            stored_proc += "BEGIN\n";
            stored_proc += $"UPDATE dbo.{this.table_name}\n";
            stored_proc += "SET\n";

            stored_proc += StoredProcBuilder.Build_UpdateList(this.table_dt);

            stored_proc += $"WHERE {this.pk_column_name}=@{this.pk_column_name}\n";
            stored_proc += "END\n";
            stored_proc += "BEGIN\n";
            stored_proc += $"INSERT INTO dbo.{this.table_name}\n";
            stored_proc += "(\n";

            columns = "";
            foreach (DataColumn col in this.table_dt.Columns)
            {
                if (columns.Length > 0) columns += "\n,";
                columns += $"[{col.ColumnName}] ";
            }
            stored_proc += StoredProcBuilder.Build_InsertColumnList(table_dt);

            stored_proc += ") VALUES (\n";

            stored_proc += StoredProcBuilder.Build_InsertValueList(table_dt);

            stored_proc += ")\n";
        }

        private void Get_Table_PK() 
        {
            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataTable result = new DataTable();

                    string query = $"SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T " +
                        "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME = T.CONSTRAINT_NAME " +
                        $"WHERE C.TABLE_NAME = '{this.table_name}' " +
                        "AND T.CONSTRAINT_TYPE = 'PRIMARY KEY'";

                    connection.Open();

                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;
                    command.ExecuteNonQuery();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);

                    adapter.Fill(result);

                    this.pk_column_name = string.Empty;

                    if (result != null
                        && result.Rows.Count > 0
                        && result.Rows[0] != null) 
                    {
                        this.pk_column_name = result.Rows[0][0].ToString();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public Dictionary<string, SqlParameter> SqlParameter_Builder(T an_object)
        {
            try
            {
                Dictionary<string, SqlParameter> result = new Dictionary<string, SqlParameter>();
                List<PropertyInfo> object_properties = an_object.GetType().GetProperties().ToList();
                object value = DBNull.Value;
                
                foreach (PropertyInfo property in object_properties)
                {
                    if (!this.table_dt.Columns.Contains(property.Name)) continue;

                    value = property.GetValue(an_object);

                    result.Add($"{property.Name}", new SqlParameter($"@{property.Name}", value));
                }

                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable Fetch(string where_conditions)
        {
            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataTable result = new DataTable();

                    connection.Open();

                    if (!string.IsNullOrEmpty(where_conditions)) 
                    {
                        where_conditions += "WHERE " + where_conditions;
                    }

                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = $"SELECT * FROM {this.table_name} {where_conditions}";
                    command.ExecuteNonQuery();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);

                    adapter.Fill(result);

                    return result;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public DataTable Fetch(string selections = "*", string where_conditions = "")
        {
            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataTable result = new DataTable();

                    connection.Open();

                    if (string.IsNullOrEmpty(selections))
                    {
                        selections = "*";
                    }

                    if (!string.IsNullOrEmpty(where_conditions))
                    {
                        where_conditions += "WHERE " + where_conditions;
                    }

                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = $"SELECT {selections} FROM {this.table_name} {where_conditions}";
                    command.ExecuteNonQuery();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);

                    adapter.Fill(result);

                    return result;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public void Delete(string where_conditions)
        {
            if (string.IsNullOrEmpty(where_conditions))
            {
                throw new Exception("Please provide DELETE condition");
            }

            using (SqlConnection connection = new SqlConnection(this.connection_string))
            {
                try
                {
                    DataTable result = new DataTable();

                    connection.Open();

                    SqlCommand command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = $"DELETE {this.table_name} WHERE {where_conditions}";
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }
            }
        }
    }
}
