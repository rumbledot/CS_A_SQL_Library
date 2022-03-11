using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using A_Common_Library.Data;
using A_SQL_Library.Attributes;

namespace A_SQL_Library
{
    public class SQL_Datatable<T>
    {
        #region variables
        //SQL database interface
        private Database_Client database;

        public DataTable this_data_table { get; protected set; }
        public DataTable this_database_table { get; protected set; }

        private DataTable columns_map_table = new DataTable();

        public string table_name_prefix { get; set; }

        private string _table_name = "new_table";
        public string table_name
        {
            get
            {
                return $"{this.table_name_prefix}{this._table_name}";
            }
            set
            {
                this._table_name = value;
            }
        }

        //this table primary column (if any)
        private string _primary_column = "";
        public string primary_column { get => _primary_column; }
        private bool _primary_column_auto = true;

        //SQL commonly used var types
        private Dictionary<string, Tuple<string, string, string>> data_mapper
        {
            get
            {
                // Add the rest of your CLR Types to SQL Types mapping here
                //Dictionary<SQL_Datatype, SQL_Default_Type_Size, SQL_Default_Value)
                Dictionary<string, Tuple<string, string, string>> dataMapper = new Dictionary<string, Tuple<string, string, string>>();
                dataMapper.Add(typeof(Int32).Name,      new Tuple<string, string, string>("INT",                string.Empty,   "0"));
                dataMapper.Add(typeof(Int16).Name,      new Tuple<string, string, string>("SMALLINT",           string.Empty,   "0"));
                dataMapper.Add(typeof(Int64).Name,      new Tuple<string, string, string>("BIGINT",             string.Empty,   "0"));
                dataMapper.Add(typeof(string).Name,     new Tuple<string, string, string>("VARCHAR",            "(250)",        string.Empty));
                dataMapper.Add(typeof(bool).Name,       new Tuple<string, string, string>("BIT",                string.Empty,   "0"));
                dataMapper.Add(typeof(DateTime).Name,   new Tuple<string, string, string>("DATETIME",           string.Empty,   DateTime.Parse("1900-01-01").ToString()));
                dataMapper.Add(typeof(float).Name,      new Tuple<string, string, string>("FLOAT",              string.Empty,   "0"));
                dataMapper.Add(typeof(decimal).Name,    new Tuple<string, string, string>("DECIMAL",            "(18, 2)",      "0"));
                dataMapper.Add("IMAGE",                 new Tuple<string, string, string>("IMAGE",              null,           null));
                dataMapper.Add("TEXT",                  new Tuple<string, string, string>("TEXT",               string.Empty,   string.Empty));
                dataMapper.Add("TIMESTAMP",             new Tuple<string, string, string>("TIMESTAMP",          null,           null));
                dataMapper.Add(typeof(Guid).Name,       new Tuple<string, string, string>("UNIQUEIDENTIFIER",   string.Empty,   string.Empty));
                //dataMapper.Add(typeof(byte[]).Name,     new Tuple<string, string, string>("TIMESTAMP",          string.Empty,   DateTime.Now.ToString()));
                return dataMapper;
            }
        }

        #endregion //variables

        public SQL_Datatable(Database_Client database)
        {
            this.table_name = typeof(T).Name;
            this.database = database;
            this.table_name_prefix = this.database.database_prefix;
        }

        public void Initialise() 
        {
            try
            {
                this.DataTableSetup();

                this.CheckDatabaseTableExists();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to initialise database table [{this.table_name}] \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        private void DataTableSetup()
        {
            this.columns_map_table.Columns.Add(new DataColumn("col_name",            typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_type",            typeof(int)));
            this.columns_map_table.Columns.Add(new DataColumn("col_datatype",        typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_sqldatatype",     typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_typeproperty",    typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_primary",         typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_default",         typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("col_nullable",        typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("index_suffix",        typeof(string)));
            this.columns_map_table.Columns.Add(new DataColumn("index_script",        typeof(string)));
        }

        private void CheckDatabaseTableExists()
        {
            this.GetModelProperties();

            if (database.QueryScalar($"SELECT COUNT(*) FROM sys.tables WHERE name = '{this.table_name}'") > 0)
            {
                this.GetTableProperties();
                this.ModelTableCompare();
            }
            else 
            {
                this.CreateUpdateDatabase(true, this.columns_map_table);
            }

        }

        //model properties and attributes
        private void GetModelProperties()
        {
            var this_object = Activator.CreateInstance<T>();
            this.this_data_table = new DataTable(table_name);

            SQL_Index sql_attr_index;
            SQL_Column sql_attr_column;
            SQL_Image sql_attr_image;
            SQL_Text sql_attr_text;
            SQL_Timestamp sql_attr_timestamp;

            var sql_primary = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Primary_Key))
                ).FirstOrDefault();

            var sql_columns = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Column))
                );

            var sql_indexes = this_object.GetType().GetProperties().Where(
                p=> Attribute.IsDefined(p, typeof(SQL_Index))
                );

            var sql_images = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Image))
                );

            var sql_texts = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Text))
                );

            var sql_timestamps = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Timestamp))
                );

            DataColumn dt_column;

            foreach (var column in sql_columns)
            {
                sql_attr_column = (SQL_Column)Attribute.GetCustomAttribute(column, typeof(SQL_Column));

                dt_column = new DataColumn();
                DataRow row = columns_map_table.NewRow();

                row["col_type"] = 1;

                //column name
                dt_column.ColumnName = string.IsNullOrEmpty(sql_attr_column.column_name) ? column.Name : sql_attr_column.column_name;
                row["col_name"] = dt_column.ColumnName;
                //column datatype
                dt_column.DataType = column.PropertyType;
                row["col_datatype"] = this.data_mapper[column.PropertyType.Name].Item1;
                row["col_typeproperty"] = string.IsNullOrEmpty(sql_attr_column.column_type_parameter) ? this.data_mapper[column.PropertyType.Name].Item2 : sql_attr_column.column_type_parameter;
                //column nullable
                if (sql_attr_column.column_nullable)
                {
                    dt_column.AllowDBNull = true;
                    dt_column.DefaultValue = this.data_mapper[column.PropertyType.Name].Item3;
                    row["col_default"] = this.data_mapper[column.PropertyType.Name].Item3;
                    row["col_nullable"] = 1;
                }
                else
                {
                    //column default
                    if (string.IsNullOrEmpty(sql_attr_column.column_default))
                    {
                        dt_column.DefaultValue = this.data_mapper[column.PropertyType.Name].Item3;
                    }
                    else 
                    {
                        dt_column.DefaultValue = sql_attr_column.column_default;
                    }

                    dt_column.AllowDBNull = false;
                    row["col_default"] = dt_column.DefaultValue;
                    row["col_nullable"] = 0;
                }

                this.this_data_table.Columns.Add(dt_column);
                this.columns_map_table.Rows.Add(row);
            }

            //SQL_Image
            foreach (var column in sql_images)
            {
                sql_attr_image = (SQL_Image)Attribute.GetCustomAttribute(column, typeof(SQL_Image));

                dt_column = new DataColumn();
                DataRow row = columns_map_table.NewRow();

                row["col_type"] = 1;

                //column name
                dt_column.ColumnName = column.Name;
                row["col_name"] = dt_column.ColumnName;
                //column datatype
                row["col_datatype"] = this.data_mapper["IMAGE"].Item1;
                row["col_typeproperty"] = this.data_mapper["IMAGE"].Item2;
                //column nullable
                row["col_nullable"] = 1;
                row["col_default"] = this.data_mapper["IMAGE"].Item3;

                this.this_data_table.Columns.Add(dt_column);
                this.columns_map_table.Rows.Add(row);
            }

            //SQL_Text
            foreach (var column in sql_texts)
            {
                sql_attr_text = (SQL_Text)Attribute.GetCustomAttribute(column, typeof(SQL_Text));

                dt_column = new DataColumn();
                DataRow row = columns_map_table.NewRow();

                row["col_type"] = 1;

                //column name
                dt_column.ColumnName = column.Name;
                row["col_name"] = dt_column.ColumnName;
                //column datatype
                row["col_datatype"] = this.data_mapper["TEXT"].Item1;
                row["col_typeproperty"] = this.data_mapper["TEXT"].Item2;
                //column nullable
                row["col_nullable"] = 1;
                row["col_default"] = this.data_mapper["TEXT"].Item3;

                this.this_data_table.Columns.Add(dt_column);
                this.columns_map_table.Rows.Add(row);
            }

            //SQL_Timestamp
            foreach (var column in sql_timestamps)
            {
                sql_attr_timestamp = (SQL_Timestamp)Attribute.GetCustomAttribute(column, typeof(SQL_Timestamp));

                dt_column = new DataColumn();
                DataRow row = columns_map_table.NewRow();

                row["col_type"] = 1;

                //column name
                dt_column.ColumnName = column.Name;
                row["col_name"] = dt_column.ColumnName;
                //column datatype
                row["col_datatype"] = this.data_mapper["TIMESTAMP"].Item1;
                row["col_typeproperty"] = this.data_mapper["TIMESTAMP"].Item2;
                //column nullable
                row["col_nullable"] = 1;
                row["col_default"] = this.data_mapper["TIMESTAMP"].Item3;

                this.this_data_table.Columns.Add(dt_column);
                this.columns_map_table.Rows.Add(row);
            }

            string col_name = "";

            //SQL_Primary_Key
            var primary_row = columns_map_table.AsEnumerable().Where(r => r.Field<string>("col_name").Equals(sql_primary.Name)).FirstOrDefault();
            if (primary_row != null)
            {
                var sql_attr_primary = (SQL_Primary_Key)Attribute.GetCustomAttribute(sql_primary, typeof(SQL_Primary_Key));

                string is_auto_increment = sql_attr_primary.is_auto_increment ? "IDENTITY(1,1) " : "";
                col_name = primary_row["col_name"].ToString();

                primary_row["col_type"] = 2;
                primary_row["col_nullable"] = 0;
                primary_row["col_primary"] = $"{is_auto_increment}PRIMARY KEY";

                this._primary_column = col_name;
                this._primary_column_auto = sql_attr_primary.is_auto_increment;
            }

            string is_clustered = "", index_suffix = "";
            int index_suffix_numbering = 0;

            foreach (var column in sql_indexes)
            {
                sql_attr_index = (SQL_Index)Attribute.GetCustomAttribute(column, typeof(SQL_Index));

                index_suffix_numbering++;
                is_clustered = sql_attr_index.is_clustered ? "CLUSTERED " : "";
                index_suffix = sql_attr_index.is_clustered ? $"C{index_suffix_numbering}" : $"N{index_suffix_numbering}";

                var rows = columns_map_table.AsEnumerable().Where(r => r.Field<string>("col_name").Equals(column.Name));

                foreach (var row in rows)
                {
                    row["col_type"] = 3;

                    col_name = row["col_name"].ToString();
                    row["index_suffix"] = index_suffix;
                    row["index_script"] = $"CREATE INDEX {is_clustered}IDX_{this.table_name}_{col_name}_{index_suffix} ON {this.table_name} ([{col_name}])";
                }
            }

            //Console.WriteLine(this.this_data_table);
            //Console.WriteLine(this.columns_map_table);
            //Console.WriteLine(new string('-', 40));
            //Console.WriteLine(new string('-', 40));
        }

        //table properties
        private void GetTableProperties()
        {
            //get database columns
            this.this_database_table = database.QueryFetch($"SELECT * FROM [{table_name}] WHERE 1=0");
            this.this_database_table.TableName = table_name;
        }

        private void ModelTableCompare()
        {
            DataTable model_datatable = this.columns_map_table.Copy();

            foreach (DataColumn column in this.this_database_table.Columns)
            {
                foreach (DataRow model_row in model_datatable.Rows)
                {
                    //if the model column already in the database than its fine, lets remove it
                    if (column.ColumnName.ToLower().Equals(model_row["col_name"].ToString().ToLower()))
                    {
                        model_datatable.Rows.Remove(model_row);
                        break;
                    }
                }
            }

            //remaining model column that needs to be added to the database
            if (model_datatable.Rows.Count > 0)
            {
                this.CreateUpdateDatabase(false, model_datatable);

                this.CreateUpdateIndexes(model_datatable);
            }

        }

        private void CreateUpdateDatabase(bool create, DataTable model_table)
        {
            if (model_table == null || model_table.Columns.Count <= 0) return;

            DataTable table = model_table;
            string query = "";
            string query_column_fragment = "", default_value = "";
            string col_name = "", col_datatype = "", col_default = "", col_nullable = "";

            foreach (DataRow row in table.Rows)
            {
                col_name = row["col_name"].ToString();

                if (string.IsNullOrEmpty(row["col_primary"].ToString()))
                {
                    col_datatype = $"{row["col_datatype"]} {row["col_typeproperty"]}";
                    if (row["col_datatype"].ToString().ToLower().Equals("datetime") 
                        || row["col_datatype"].ToString().ToLower().Equals("timestamp"))
                    {
                        default_value = $"'{row["col_default"]}'";
                    }
                    else if (row["col_datatype"].ToString().ToLower().Equals("varchar"))
                    {
                        if (string.IsNullOrEmpty(row["col_default"].ToString())
                            || string.IsNullOrWhiteSpace(row["col_default"].ToString()))
                        {
                            default_value = "''";
                        }
                        else 
                        {
                            default_value = $"'{row["col_default"]}'";
                        }
                    }
                    else if (row["col_datatype"].ToString().ToLower().Equals("bit"))
                    {
                        default_value = row["col_default"].ToString().ToLower().Equals("false") ? "0" : "1";
                    }
                    else
                    {
                        default_value = row["col_default"].ToString();
                    }
                    col_default = string.IsNullOrEmpty(row["col_default"].ToString()) ? "" : $" DEFAULT({default_value})";
                    col_nullable = Convert.ToInt32(row["col_nullable"]) == 0 ? " NOT NULL" : "";
                }
                else
                {
                    col_datatype = $"{row["col_datatype"]} {row["col_typeproperty"]} {row["col_primary"]}";
                    col_default = "";
                    col_nullable = "";
                }

                query_column_fragment += $"{col_name} {col_datatype}{col_default}{col_nullable},";
            }

            if (create)
            {
                query_column_fragment = query_column_fragment.Substring(0, query_column_fragment.Length - 1);
                query = $"CREATE TABLE {table_name} ({query_column_fragment})";
            }
            else 
            {
                query_column_fragment = query_column_fragment.Substring(0, query_column_fragment.Length - 1);
                query = $"ALTER TABLE {table_name} ADD {query_column_fragment}";
            }

            this.database.QueryExecute(query);

            //Console.WriteLine(query);
            //Console.WriteLine(new string('-', 40));
            //Console.WriteLine(new string('-', 40));
        }

        private void CreateUpdateIndexes(DataTable model_table)
        {
            DataTable table = model_table;

            var new_indexes = table.AsEnumerable().Where(r => r.Field<int>("col_type") == 2);

            if (new_indexes.Count<DataRow>() > 0)
            {
                foreach (DataRow row in new_indexes)
                {
                    this.database.QueryExecute(row["index_script"].ToString());
                }
            }
        }

        public void CreateTableType() 
        {
            var this_object = Activator.CreateInstance<T>();
            string query = "";

            using (StreamWriter writer = new StreamWriter($@"\SQL_Scripts\tt_{this.table_name}.sql"))
            {
                try
                {
                    StringBuilder builder = new StringBuilder();

                    var sql_columns = this_object.GetType().GetProperties().Where(
                        p => Attribute.IsDefined(p, typeof(SQL_Column))
                        );

                    var sql_images = this_object.GetType().GetProperties().Where(
                        p => Attribute.IsDefined(p, typeof(SQL_Image))
                        );

                    var sql_texts = this_object.GetType().GetProperties().Where(
                        p => Attribute.IsDefined(p, typeof(SQL_Text))
                        );

                    builder.Append($"IF TYPE_ID(N'TABLETYPE_{this.table_name}') IS NULL ");
                    builder.AppendLine($"BEGIN ");
                    builder.AppendLine($"CREATE TYPE dbo.TABLETYPE_{this.table_name} AS TABLE ( ");

                    foreach (var column in sql_columns)
                    {
                        builder.AppendLine($"{column.Name} {this.data_mapper[column.PropertyType.Name].Item1}");
                    }

                    foreach (var column in sql_images)
                    {
                        builder.AppendLine($"{column.Name} {this.data_mapper["IMAGE"].Item1}");
                    }

                    foreach (var column in sql_texts)
                    {
                        builder.AppendLine($"{column.Name} {this.data_mapper["TEXT"].Item1}");
                    }

                    builder.AppendLine(") ");
                    builder.AppendLine("END");
                    writer.WriteLineAsync(builder.ToString());
                }
                catch (Exception ex)
                {
                    throw new Exception($"SQL_Datatable - CreateTableType Exception\n{ex.Message}\n{ex.StackTrace}");
                }
            }
        }

        #region CRUD

        //used to 'seed' empty/new dataabse table
        public void Seed(T item) 
        {
            using (SqlConnection conn = new SqlConnection(this.database.connection_string))
            {
                Tuple<string, List<SqlParameter>> insert_query;
                string query = "";
                List<SqlParameter> parameters;

                try
                {
                    conn.Open();

                    insert_query = this.InsertQuery(item);
                    query = $"IF NOT EXISTS (SELECT * FROM {this.table_name}) BEGIN ";
                    query += insert_query.Item1;
                    query += $" END";

                    parameters = insert_query.Item2;

                    SqlCommand comm = new SqlCommand();
                    comm.Connection = conn;
                    comm.CommandType = CommandType.Text;
                    comm.CommandText = query;
                    comm.Parameters.AddRange(parameters.ToArray());
                    comm.ExecuteNonQuery();

                    conn.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception($"SQL_Table failed to seed table [{this.table_name}] \n {ex.Message} \n\n {ex.StackTrace}");
                }
                finally
                {
                    if (conn.State == ConnectionState.Open) conn.Close();
                }
            }
        }

        //user is expected to pass a column name that is the same as object's property name and db column's name
        public void Add(T item, string condition = "")
        {
            Tuple<string, List<SqlParameter>> sql_query;

            try
            {
                sql_query = this.GetAddQuery(item, condition);

                //execute the query
                database.QueryExecute(sql_query.Item1, sql_query.Item2);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to auto insert/update object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public Tuple<string, List<SqlParameter>> GetAddQuery(T item, string condition = "")
        {
            string query = "", query_variables = "";
            object value = DBNull.Value;
            int total_prop = 0, index = 0, primary_value = -1;
            Dictionary<string, SqlParameter> variables = new Dictionary<string, SqlParameter>();
            List<SqlParameter> parameters = new List<SqlParameter>();

            try
            {
                if (!string.IsNullOrEmpty(this._primary_column))
                {
                    primary_value = Convert.ToInt32(item.GetType().GetProperty(this._primary_column).GetValue(item));
                }
                //else 
                //{
                //    var sql_first_column = item.GetType().GetProperties().FirstOrDefault(
                //            p => Attribute.IsDefined(p, typeof(SQL_Column))
                //        );
                    
                //    string first_value = "";

                //    if (sql_first_column.PropertyType.Name.ToLower().Equals("string") || sql_first_column.PropertyType.Name.ToLower().Equals("datetime")) first_value = $"'{sql_first_column.GetValue(item)}'";
                //    else first_value = $"{sql_first_column.GetValue(item)}";

                //    int exists = this.database.QueryScalar($"SELECT {sql_first_column.Name} FROM {this.table_name} WHERE [{sql_first_column.Name}]={first_value}");
                //    primary_value = exists;
                //    this._primary_column = sql_first_column.Name;
                //}

                //assuming primary is always an int and set default to -1
                //if the value is greater than 0 means its an update
                if (primary_value <= 0)
                {
                    variables = this.QueryVarValueBuilder(item, true);
                    total_prop = variables.Count;

                    query = $"INSERT INTO {this.table_name} VALUES (";

                    //query += this._primary_column_auto ? "" : $"@{this._primary_column}";

                    foreach (var var in variables)
                    {
                        query += $"@{var.Key}";

                        parameters.Add(var.Value);

                        //check whether need to add ,
                        index++;
                        if (index < total_prop)
                        {
                            query += ",";
                            query_variables += ",";
                        }
                    }
                    query += ")";
                }
                else
                {
                    variables = this.QueryVarValueBuilder(item, true);
                    total_prop = variables.Count;

                    query = $"UPDATE {this.table_name} SET ";

                    foreach (var var in variables)
                    {
                        query += $"{var.Key}=@{var.Key}";

                        parameters.Add(var.Value);

                        //check whether need to add ,
                        index++;
                        if (index < total_prop)
                        {
                            query += ",";
                        }
                    }
                    condition = string.IsNullOrEmpty(condition) ? $"{this._primary_column}={primary_value}" : condition;
                    query += $" WHERE {condition}";
                }

                //Console.WriteLine(query);

                return new Tuple<string, List<SqlParameter>>(query, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to get auto insert/update query \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        private Dictionary<string, SqlParameter> QueryVarValueBuilder(T new_item, bool create = false) 
        {
            Dictionary<string, SqlParameter> result = new Dictionary<string, SqlParameter>();
            PropertyInfo[] this_object_properties = new_item.GetType().GetProperties();
            object value = DBNull.Value;

            try
            {
                var sql_columns = new_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Column))
                );

                if (!this._primary_column_auto) 
                {
                    value = new_item.GetType().GetProperty(this._primary_column).GetValue(new_item);
                    result.Add($"{this._primary_column}", new SqlParameter($"@{this._primary_column}", value));
                }

                foreach (var property in sql_columns)
                {
                    if (create && property.Name.ToLower().Equals(this._primary_column.ToLower()))
                    {
                        continue;
                    }

                    value = property.GetValue(new_item);
                    if (value == null || value == DBNull.Value) value = this.data_mapper[property.PropertyType.Name].Item3;

                    result.Add($"{property.Name}", new SqlParameter($"@{property.Name}", value));
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table QueryVarVariableBuilder exception \n {ex.Message} \n\n {ex.StackTrace}");
            }

        }

        public void Insert(T new_item)
        {
            try
            {
                var query = this.InsertQuery(new_item);

                //execute the query
                database.QueryExecute(query.Item1, query.Item2);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to insert object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public Tuple<string, List<SqlParameter>> InsertQuery(T new_item)
        {
            string query = "";
            string query_columns = "";

            PropertyInfo[] this_object_properties = new_item.GetType().GetProperties();
            List<SqlParameter> sql_parameters = new List<SqlParameter>();
            int index = 0, total_prop = 0;
            object value = DBNull.Value;

            var sql_columns = new_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Column)))
                .ToList();

            sql_columns.AddRange(new_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Image)))
                .ToList()
                );

            sql_columns.AddRange(new_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Text)))
                .ToList()
                );

            total_prop = sql_columns.Count();

            //INSERT command columns part
            if (!this._primary_column_auto)
            {
                query_columns += $"{this._primary_column},";
            }

            index = 0;
            foreach (var property in sql_columns) 
            {
                if (property.Name.ToLower().Equals(this._primary_column.ToLower()))
                {
                    total_prop -= 1;
                    continue;
                }

                query_columns += $"{property.Name}";

                index++;
                if (index < total_prop)
                {
                    query_columns += ",";
                }
            }

            //assembles
            query = $"INSERT INTO {this.table_name} ({query_columns}) VALUES (";

            //INSERT command values part
            if (!this._primary_column_auto)
            {
                query += $"@{this._primary_column},";

                value = new_item.GetType().GetProperty(this._primary_column).GetValue(new_item);
                sql_parameters.Add(new SqlParameter($"@{this._primary_column}", value));
            }

            total_prop = sql_columns.Count();
            index = 0;
            foreach (var property in sql_columns)
            {
                if (property.Name.ToLower().Equals(this._primary_column.ToLower()))
                {
                    total_prop -= 1;
                    continue;
                }

                query += $"@{property.Name}";
                value = property.GetValue(new_item);
                if (value == null || value == DBNull.Value) value = this.data_mapper[property.PropertyType.Name].Item3;

                sql_parameters.Add(new SqlParameter($"@{property.Name}", value));

                //check whether need to add ,
                index++;
                if (index < total_prop)
                {
                    query += ",";
                }
            }
            query += ")";

            return new Tuple<string, List<SqlParameter>>(query, sql_parameters);
        }

        public void Update(T updated_item, string condition)
        {
            try
            {
                var query = this.UpdateQuery(updated_item, condition);

                //execute the query
                database.QueryExecute(query.Item1, query.Item2);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to insert object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public Tuple<string, List<SqlParameter>> UpdateQuery(T updated_item, string condition = "") 
        {
            string query = "";
            PropertyInfo[] this_object_properties = updated_item.GetType().GetProperties();
            List<SqlParameter> sql_parameters = new List<SqlParameter>();
            int index = 0, total_prop = 0;
            object value = DBNull.Value;

            var sql_columns = updated_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Column)))
                .ToList();

            sql_columns.AddRange(updated_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Image)))
                .ToList()
                );

            sql_columns.AddRange(updated_item.GetType().GetProperties().Where(
                    p => Attribute.IsDefined(p, typeof(SQL_Text)))
                .ToList()
                );

            total_prop = sql_columns.Count();

            query = $"UPDATE {this.table_name} SET ";

            //if (!this._primary_column_auto)
            //{
            //    query += $"{this._primary_column}=@{this._primary_column},";

            //    value = updated_item.GetType().GetProperty(this._primary_column).GetValue(updated_item);
            //    sql_parameters.Add(new SqlParameter($"@{this._primary_column}", value));
            //}

            foreach (var property in sql_columns)
            {
                if (property.Name.ToLower().Equals(this._primary_column.ToLower()))
                {
                   total_prop -= 1;
                    continue;
                }

                query += $"{property.Name}=@{property.Name}";
                value = property.GetValue(updated_item);
                if (value == null || value == DBNull.Value) value = this.data_mapper[property.PropertyType.Name].Item3;

                sql_parameters.Add(new SqlParameter($"@{property.Name}", value));

                //check whether need to add ,
                index++;
                if (index < total_prop) query += ",";
            }

            if (!string.IsNullOrEmpty(condition))
            {
                query += $" WHERE {condition}";
            }

            return new Tuple<string, List<SqlParameter>>(query, sql_parameters);
        }

        public T Load(string condition)
        {
            try
            {
                DataTable result = database.QueryFetch($"SELECT TOP 1 * FROM {this.table_name} WHERE {condition}");

                if (result == null || result.Rows.Count <= 0) return default(T);

                return result.Rows[0].ToConstructorlessObject<T>();
            }
            catch
            {
                //throw new Exception($"Load {typeof(T).Name} failed. \n\n No result found.");
                return default(T);
            }
        }
        public DataTable LoadAll(string condition)
        {
            DataTable result = database.QueryFetch($"SELECT * FROM {this.table_name} WHERE {condition}");

            if (result == null || result.Rows.Count <= 0) return null;

            return result;
        }

        public DataTable LoadAll()
        {
            return database.QueryFetch($"SELECT * FROM {this.table_name}");
        }

        public void Delete(string condition)
        {
            database.QueryExecute($"DELETE FROM {this.table_name} WHERE {condition}");
        }

        #endregion //CRUD
    }
}
