using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using A_Common_Library.Data;
using A_SQL_Library.Attributes;

namespace A_SQL_Library
{
    public enum KeyTypes
    {
        primary,
        index,
        none
    }

    public class SQL_Table<T>
    {
        #region variables
        //SQL database interface
        private Database_Client database;

        public string table_name_prefix { get; set; } = "tbl_";

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

        //Mapper dictionaries
        //A Dictionary of (
        //<actual column name in database>,
        //<object property name (the object property name for this column), the script to create them, is the column a primary key/index/regular column?>
        //)
        private Dictionary<string, Tuple<string, string, KeyTypes>> _object_properties;
        public Dictionary<string, Tuple<string, string, KeyTypes>> object_properties
        {
            get { return this._object_properties; }
            set { this._object_properties = value; }
        }

        //Mapper dictionaries
        //A Dictionary of (
        //<actual column name in database>,
        //<object's property name>
        //)
        private Dictionary<string, string> _database_columns;

        //Mapper dictionaries
        //A Dictionary of (
        //<actual column name in database>,
        //<create index script for above column name>
        //)
        private Dictionary<string, string> _index_columns;

        //this table primary column (if any)
        private string _primary_column = "";

        //SQL commonly used var types
        private Dictionary<string, Tuple<string, string, string>> _data_mapper
        {
            get
            {
                // Add the rest of your CLR Types to SQL Types mapping here
                Dictionary<string, Tuple<string, string, string>> dataMapper = new Dictionary<string, Tuple<string, string, string>>();
                dataMapper.Add(typeof(Int32).Name, new Tuple<string, string, string>("SMALLINT", "", "0"));
                dataMapper.Add(typeof(Int16).Name, new Tuple<string, string, string>("INT", "", "0"));
                dataMapper.Add(typeof(Int64).Name, new Tuple<string, string, string>("BIGINT", "", "0"));
                dataMapper.Add(typeof(string).Name, new Tuple<string, string, string>("VARCHAR", "(250)", "empty"));
                dataMapper.Add(typeof(bool).Name, new Tuple<string, string, string>("BIT", "", "0"));
                dataMapper.Add(typeof(DateTime).Name, new Tuple<string, string, string>("DATETIME", "", DateTime.Now.ToString()));
                dataMapper.Add(typeof(float).Name, new Tuple<string, string, string>("FLOAT", "", "0"));
                dataMapper.Add(typeof(decimal).Name, new Tuple<string, string, string>("DECIMAL", "(12, 2)", "0"));
                dataMapper.Add(typeof(Guid).Name, new Tuple<string, string, string>("UNIQUEIDENTIFIER", "", ""));
                return dataMapper;
            }
        }

        #endregion //variables

        public SQL_Table(Database_Client database)
        {
            this.table_name = typeof(T).Name;
            this.database = database;
        }

        public void Initialise() 
        {
            try
            {
                //get current object's properties
                //may differ from table columns in the database
                var this_object = Activator.CreateInstance<T>();

                //first, we need to check both sides, the object and the database
                Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>> this_object_properties_and_index = this.GetObjectProperties(this_object);
                this._object_properties = this_object_properties_and_index.Item1;
                //Dictionary<string, string> index_columns = this_object_properties_and_index.Item2;
                this._index_columns = this_object_properties_and_index.Item2;

                if (database.QueryScalar(this.CheckTableExistsScript()) <= 0)
                {
                    //Console.WriteLine(CreateTableScript());
                    //Console.WriteLine($"Created new table {this.table_name}");
                    database.QueryExecute(this.CreateTableScript());

                    //create indexes (if any)
                    this.CreateIndexColumns(this._index_columns);

                    //get the actual columns from database table
                    this._database_columns = this.CompareTableToObject();
                }
                else
                {
                    //Console.WriteLine($"{this.table_name} exists. Lets check, update if needed");
                    //Console.WriteLine($"Add new column {this.CompareTableToObject()}");

                    //get the actual columns from database table
                    Dictionary<string, string> new_database_columns = this.CompareTableToObject();

                    //i can only add new properties to table's columns
                    //too risky to modify or drop column programmatically
                    //leave this decision to db admin and do it in ssms
                    if (new_database_columns != null && new_database_columns.Count > 0)
                    {
                        Tuple<string, Dictionary<string, string>> need_to_add = this.UpdateDatabaseTableColumnsScript(new_database_columns);
                        string script = need_to_add.Item1;
                        Dictionary<string, string> new_indexes = need_to_add.Item2;

                        Console.WriteLine(script);
                        database.QueryExecute(script);

                        foreach (string key in new_indexes.Keys)
                        {
                            script = this._index_columns[key];
                            Console.WriteLine(script);
                            database.QueryExecute(script);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to initialise database table {this.table_name} \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        //I'm trying to kill two birds with one stone here
        //One loop of this abstract object's properties will generate information for object's<col name, <prop name, script, key type>> and table's index<col name, script>
        public Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>> GetObjectProperties(object object_to_map)
        {
            //initialise and reset _field_info
            Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>> result;
            Dictionary<string, Tuple<string, string, KeyTypes>> this_object_properties = new Dictionary<string, Tuple<string, string, KeyTypes>>();
            Dictionary<string, string> this_index_columns = new Dictionary<string, string>();

            //local vars
            SQL_Column sql_col_attr;
            SQL_Index sql_col_index;
            SQL_Primary_Key sql_col_primary;
            string col_name = "", col_type = "", col_param = "", col_default = "", col_primary = "", col_null = "";
            string is_clustered = "", index_suffix = "";
            int index_suffix_numbering = 0;
            bool already_has_primary = false;
            KeyTypes key_type = KeyTypes.none;

            //iterate object's properties
            foreach (PropertyInfo p in object_to_map.GetType().GetProperties())
            {
                //check if property has an atrribute
                sql_col_attr = (SQL_Column)Attribute.GetCustomAttribute(p, typeof(SQL_Column));
                sql_col_index = (SQL_Index)Attribute.GetCustomAttribute(p, typeof(SQL_Index));
                sql_col_primary = (SQL_Primary_Key)Attribute.GetCustomAttribute(p, typeof(SQL_Primary_Key));
                key_type = KeyTypes.none;

                //skip property with no attribute

                //this will check SQL_Column attribute and generate object's<col name, <prop name, script>>
                if (sql_col_attr != null)
                {
                    //check desired column name
                    if (string.IsNullOrEmpty(sql_col_attr.column_name))
                    {
                        col_name = p.Name;
                    }
                    else
                    {
                        col_name = sql_col_attr.column_name;
                    }

                    //check property type and try convert it to sql data type
                    if (sql_col_attr.column_type == null)
                    {
                        if (_data_mapper.ContainsKey(p.PropertyType.Name))
                        {
                            col_type = _data_mapper[p.PropertyType.Name].Item1;
                        }
                        else
                        {
                            throw new Exception($"{col_type} don't have a compatible SQL type");
                        }
                    }
                    else
                    {
                        col_type = _data_mapper[sql_col_attr.column_type.Name].Item1;
                    }

                    //check if user provides additional sql data type specification
                    if (string.IsNullOrEmpty(sql_col_attr.column_type_parameter))
                    {
                        col_param = _data_mapper[p.PropertyType.Name].Item2;
                    }
                    else
                    {
                        col_param = $"({sql_col_attr.column_type_parameter})";
                    }

                    //check if user provides default value
                    if (!sql_col_attr.column_nullable)
                    {
                        if (col_type.Equals("VARCHAR")) 
                        {
                            if (string.IsNullOrEmpty(sql_col_attr.column_default))
                            {
                                col_default = $" DEFAULT '{_data_mapper[p.PropertyType.Name].Item3}'";
                            }
                            else
                            {
                                col_default = $" DEFAULT '{sql_col_attr.column_default}'";
                            }
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(sql_col_attr.column_default))
                            {
                                col_default = $" DEFAULT {_data_mapper[p.PropertyType.Name].Item3}";
                            }
                            else
                            {
                                col_default = $" DEFAULT {sql_col_attr.column_default}";
                            }
                        }
                    }

                    //check if user specified this column as nullable
                    if (sql_col_attr.column_nullable)
                    {
                        col_null = "";
                    }
                    else
                    {
                        col_null = " NOT NULL";
                    }

                    //this will check SQL_Primary attribute will enforce not null
                    if (sql_col_primary != null && !already_has_primary)
                    {
                        string is_auto_increment = sql_col_primary.is_auto_increment ? " IDENTITY(1,1) " : "";
                        col_primary = $"{is_auto_increment}PRIMARY KEY";
                        col_default = "";
                        col_null = "";
                        this._primary_column = col_name;
                        key_type = KeyTypes.primary;
                        already_has_primary = true;
                    }

                    //this property's <column name, <property name, script, is_primarykey>>
                    this_object_properties.Add(col_name, new Tuple<string, string, KeyTypes>(p.Name, $"{col_type}{col_param}{col_default}{col_null}{col_primary}", key_type));
                    col_primary = ""; key_type = KeyTypes.none;
                }

                //this will check SQL_Index attribute and generate table's indexes<col name, script>
                if (sql_col_index != null)
                {
                    index_suffix_numbering = this_index_columns.Count;
                    is_clustered = sql_col_index.is_clustered ? "CLUSTERED " : "";
                    index_suffix = sql_col_index.is_clustered ? $"C{index_suffix_numbering}" : $"N{index_suffix_numbering}";

                    this_index_columns.Add(col_name, $"CREATE INDEX {is_clustered}IDX_{this.table_name}_{col_name}_{index_suffix} ON {this.table_name} ([{col_name}])");
                }
            }

            //NOTE
            //at last we are returning both information wrap in a Tuple object
            //A Tuple of (
            //<dictionary of column name - (property name, script to create them)>
            //<dictionary of column name - script to create them>
            //)
            result = new Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>>(this_object_properties, this_index_columns);

            return result;
        }

        public string CheckTableExistsScript()
        {
            return $"SELECT COUNT(*) FROM sys.tables WHERE name = '{this.table_name}'";
        }

        //after we have the object's properties information from [GetObjectProperties] function
        //we can use it to  generate sql script
        public string CreateTableScript()
        {
            //local vars
            int index = 0, total_prop = typeof(T).GetProperties().Length;

            //build the script using StringBuilder
            StringBuilder script = new StringBuilder();

            script.AppendLine("CREATE TABLE " + this.table_name);
            script.AppendLine("(");

            //iterate object's properties
            foreach (string col_name in object_properties.Keys)
            {
                    script.Append($"[{col_name}] {object_properties[col_name].Item2}");

                    //check whether need to add ,
                    index++;
                    if (index < total_prop) script.Append(",");
                    script.AppendLine();
            }
            //conclude the script
            script.AppendLine(")");

            return script.ToString();
        }

        //after we have the object's properties information from [GetObjectProperties] function
        //we can use generated sql script of each column to creating indexes in the table
        private void CreateIndexColumns(Dictionary<string, string> index_columns)
        {
            if (index_columns != null && index_columns.Count > 0) 
            {
                foreach (string col in index_columns.Keys)
                {
                    database.QueryExecute(index_columns[col]);
                }
            }
        }

        public string GetTableColumnsScript()
        {
            return $"SELECT c.[COLUMN_NAME], c.[IS_NULLABLE], c.[DATA_TYPE], c.[COLUMN_DEFAULT], "+
                "c.[CHARACTER_MAXIMUM_LENGTH], " +
                "c.[NUMERIC_PRECISION], c.[NUMERIC_SCALE],  " +
                "c.[DATETIME_PRECISION], " +
                "(CASE " +
                "WHEN ku.COLUMN_NAME = c.COLUMN_NAME THEN 1 " +
                "ELSE 0 END) AS[PRIMARY] " +
                "FROM INFORMATION_SCHEMA.COLUMNS AS c " +
                "LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku ON ku.[TABLE_NAME] = c.[TABLE_NAME] " +
                $"WHERE c.[TABLE_NAME]='{this.table_name}' ORDER BY c.[ORDINAL_POSITION]";
        }

        //This class will try to compare C# model object with DB table
        //Compare object's col names with table's col names
        //if object has new col names we will return it in
        //A Dictionary of <col name, col data type parameters script>
        public Dictionary<string, string> CompareTableToObject()
        {
            //local vars
            List<string> database_col_names = new List<string>();
            Dictionary<string, string> new_column = new Dictionary<string, string>();

            //get table actual column names
            DataTable database_column = database.QueryFetch(GetTableColumnsScript());
            foreach (DataRow row in database_column.Rows)
            {
                database_col_names.Add(row["COLUMN_NAME"].ToString());
            }

            //compare them to what we have
            foreach (string object_column_name in this.object_properties.Keys)
            {
                if (database_col_names.Contains(object_column_name)) continue;
                new_column.Add(object_column_name, object_properties[object_column_name].Item2);
            }

            return new_column;
        }

        //after [CompareTableToObject] we can add object's new proprty as column in the database table
        //this will takes in <column name, script of data type required parameter>
        public Tuple<string, Dictionary<string, string>> UpdateDatabaseTableColumnsScript(Dictionary<string, string> new_column)
        {
            //local vars
            int index = 0;
            int total_prop = new_column.Count;
            Dictionary<string, string> new_indexes = new Dictionary<string, string>();

            //build alter table script
            StringBuilder script = new StringBuilder();
            script.AppendLine($"ALTER TABLE {this.table_name}");

            //iterate object's properties
            foreach (string col_name in new_column.Keys)
            {
                //get new column name and data type parameters
                script.Append($"ADD {col_name} {new_column[col_name].ToString()}");

                //also check if this column is an index
                //lets create it right AWAY!!!
                //we just returning alter table script
                //NOTE:not a good idea!
                if (this._index_columns.ContainsKey(col_name)) 
                {
                    //database.QueryExecute(this._index_columns[col_name]);
                    new_indexes.Add(col_name, this._index_columns[col_name]);
                }

                //check whether need to add ,
                index++;
                if (index < total_prop) script.Append(",");
                script.AppendLine();
            }
            //conclude the script

            return new Tuple<string, Dictionary<string, string>>(script.ToString(), new_indexes);
        }

        #region CRUD Operations

        //user is expected to pass a column name that is the same as object's property name and db column's name
        public void Add(T item, string column = "")
        {
            try
            {
                //local vars
                string query = "";
                string condition = "";
                object value = null;
                string object_property_name = "";

                if (!string.IsNullOrEmpty( column))
                {
                    object_property_name = this.object_properties[column].Item1;
                }
                else if (!string.IsNullOrEmpty(this._primary_column))
                {
                    object_property_name = this.object_properties[this._primary_column].Item1;
                }
                else //lets try find a comparison automatically 
                {
                    object_property_name = this.object_properties.ElementAt(0).Key;
                }

                //build the query script
                //if the value is string/datetime we need to add ''
                if (typeof(T).GetProperty(object_property_name).PropertyType.Name.Equals("String"))
                {
                    value = (typeof(T).GetProperty(object_property_name).GetValue(item) == null) ? "" : typeof(T).GetProperty(object_property_name).GetValue(item);
                    condition = $"[{object_property_name}]='{value}'";
                } 
                else if (typeof(T).GetProperty(object_property_name).PropertyType.Name.Equals("DateTime")) 
                {
                    value = (typeof(T).GetProperty(object_property_name).GetValue(item) == null) ? DateTime.Now : typeof(T).GetProperty(object_property_name).GetValue(item);
                    condition = $"[{object_property_name}]='{value}'";
                }
                else
                {
                    value = (typeof(T).GetProperty(object_property_name).GetValue(item) == null) ? 0 : typeof(T).GetProperty(object_property_name).GetValue(item);
                    condition = $"[{object_property_name}]={value}";
                }

                query = $"SELECT COUNT(*) FROM [{this.table_name}] WHERE {condition}";

                //Console.WriteLine(query);
                int exists = this.database.QueryScalar(query);
                if (exists > 0)
                {
                    this.Update(item, condition);
                }
                else
                {
                    this.Insert(item);
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to auto insert/update object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public void Insert(T new_item)
        {
            try
            {
                //local vars
                Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>> this_object_properties_and_index = this.GetObjectProperties(new_item);
                Dictionary<string, Tuple<string, string, KeyTypes>> this_object_properties = this_object_properties_and_index.Item1;
                Dictionary<string, string> this_index_columns = this_object_properties_and_index.Item2;
                this_object_properties_and_index = null;
                List<SqlParameter> sql_parameters = new List<SqlParameter>();
                //int index = 0, total_prop = !string.IsNullOrEmpty(this._primary_column) ? this_object_properties.Count - 1 : this_object_properties.Count;
                int index = 0, total_prop = this_object_properties.Count;

                StringBuilder script = new StringBuilder();

                script.AppendLine($"INSERT INTO {this.table_name}");
                script.AppendLine("VALUES (");

                foreach (string p in this_object_properties.Keys)
                {
                    if (this.object_properties[p].Item3 == KeyTypes.primary) total_prop -= 1;

                    if (this.object_properties.Keys.Contains(p) && this.object_properties[p].Item3 != KeyTypes.primary)
                    {
                        script.AppendLine($"@{p}");
                        PropertyInfo col_prop = typeof(T).GetProperty(this._object_properties[p].Item1);
                        var col_val = Convert.ChangeType(col_prop.GetValue(new_item), col_prop.PropertyType);
                        sql_parameters.Add(new SqlParameter($"@{p}", col_val));
                        //script.AppendLine($"{col_val}");

                        //check whether need to add ,
                        index++;
                        if (index < total_prop) script.Append(",");
                        script.AppendLine();
                    }
                }
                //conclude script
                script.AppendLine(")");

                Console.WriteLine(script.ToString());

                //execute the query
                database.QueryExecute(script.ToString(), sql_parameters);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to insert object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public void Update(T updated_item, string condition)
        {
            try
            {
                //local vars
                Tuple<Dictionary<string, Tuple<string, string, KeyTypes>>, Dictionary<string, string>> this_object_properties_and_index = this.GetObjectProperties(updated_item);
                Dictionary<string, Tuple<string, string, KeyTypes>> this_object_properties = this_object_properties_and_index.Item1;
                Dictionary<string, string> this_index_columns = this_object_properties_and_index.Item2;
                this_object_properties_and_index = null;
                List<SqlParameter> sql_parameters = new List<SqlParameter>();
                int index = 0, total_prop = this_object_properties.Count;

                StringBuilder script = new StringBuilder();

                script.AppendLine($"UPDATE {this.table_name}");
                script.AppendLine("SET ");

                foreach (string p in this_object_properties.Keys)
                {
                    if (this.object_properties[p].Item3 == KeyTypes.primary) total_prop -= 1;

                    if (this.object_properties.Keys.Contains(p) && this.object_properties[p].Item3 != KeyTypes.primary)
                    {
                        script.AppendLine($"{p}=@{p}");
                        PropertyInfo col_prop = typeof(T).GetProperty(this._object_properties[p].Item1);
                        var col_val = Convert.ChangeType(col_prop.GetValue(updated_item), col_prop.PropertyType);
                        sql_parameters.Add(new SqlParameter($"@{p}", col_val));
                        //script.AppendLine($"{col_val}");

                        //check whether need to add ,
                        index++;
                        if (index < total_prop) script.Append(",");
                        script.AppendLine();
                    }
                }
                script.AppendLine($"WHERE {condition}");
                //conclude script

                Console.WriteLine(script.ToString());

                //execute the query
                database.QueryExecute(script.ToString(), sql_parameters);
            }
            catch (Exception ex)
            {
                throw new Exception($"SQL_Table failed to update object \n {ex.Message} \n\n {ex.StackTrace}");
            }
        }

        public T Load(string condition)
        {
            try
            {
                DataRow result = database.QueryFetch($"SELECT * FROM {this.table_name} WHERE {condition}").Rows[0];
                return result.ToConstructorlessObject<T>();
            }
            catch
            {
                throw new Exception($"Load {typeof(T).Name} failed. \n\n No result found.");
            }
        }

        public void Delete(string condition)
        {
            database.QueryExecute($"DELETE FROM {this.table_name} WHERE {condition}");
        }

        public DataTable LoadAll()
        {
            return database.QueryFetch($"SELECT * FROM {this.table_name}");
        }

        #endregion //CRUD Operations
    }
}
