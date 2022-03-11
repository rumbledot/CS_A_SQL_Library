using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using A_SQL_Library.Attributes;

namespace A_SQL_Library
{
    public class SQL_Rowtable<T>
    {
        #region variables
        //SQL database interface
        private Database_Client database;

        public DataTable this_data_table { get; protected set; }
        public DataTable this_database_table { get; protected set; }

        private DataTable columns_map_table = new DataTable();

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

        //this table primary column (if any)
        private string _primary_column = "";

        //SQL commonly used var types
        private Dictionary<string, Tuple<string, string, string>> data_mapper
        {
            get
            {
                // Add the rest of your CLR Types to SQL Types mapping here
                //Dictionary<SQL_Datatype, SQL_Default_Type_Size, SQL_Default_Value)
                Dictionary<string, Tuple<string, string, string>> dataMapper = new Dictionary<string, Tuple<string, string, string>>();
                dataMapper.Add(typeof(Int32).Name, new Tuple<string, string, string>("INT", string.Empty, "0"));
                dataMapper.Add(typeof(Int16).Name, new Tuple<string, string, string>("INT", string.Empty, "0"));
                dataMapper.Add(typeof(Int64).Name, new Tuple<string, string, string>("INT", string.Empty, "0"));
                dataMapper.Add(typeof(string).Name, new Tuple<string, string, string>("VARCHAR", "(250)", string.Empty));
                dataMapper.Add(typeof(bool).Name, new Tuple<string, string, string>("BIT", string.Empty, "0"));
                dataMapper.Add(typeof(DateTime).Name, new Tuple<string, string, string>("DATETIME", string.Empty, DateTime.Parse("1753/01/01").ToString()));
                dataMapper.Add(typeof(float).Name, new Tuple<string, string, string>("FLOAT", string.Empty, "0"));
                dataMapper.Add(typeof(decimal).Name, new Tuple<string, string, string>("DECIMAL", "(18, 2)", "0"));
                dataMapper.Add(typeof(Guid).Name, new Tuple<string, string, string>("UNIQUEIDENTIFIER", string.Empty, string.Empty));
                dataMapper.Add(typeof(byte[]).Name, new Tuple<string, string, string>("TIMESTAMP", string.Empty, DateTime.Now.ToString()));
                return dataMapper;
            }
        }

        #endregion //variables

        public SQL_Rowtable(Database_Client database, object model)
        {
            this.table_name = model.GetType().Name;
            this.database = database;
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
            var this_object = Activator.CreateInstance<T>();
            this.this_data_table = new DataTable(table_name);
            this.this_data_table.Columns.Add(new DataColumn("id", typeof(int)));
            this.this_data_table.Columns.Add(new DataColumn("field", typeof(string)));
            this.this_data_table.Columns.Add(new DataColumn("value", typeof(string)));
        }

        private void CheckDatabaseTableExists()
        {
            GetModelProperties();

            if (database.QueryScalar($"SELECT COUNT(*) FROM sys.tables WHERE name = '{this.table_name}'") > 0)
            {
                GetTableProperties();
                ModelTableCompare();
            }
            else
            {
                CreateUpdateDatabase(true, this.columns_map_table);
            }

        }

        private void GetModelProperties()
        {
            var this_object = Activator.CreateInstance<T>();
            this.this_data_table = new DataTable(table_name);

            SQL_Column sql_attr_column;
            DataRow row;

            var sql_columns = this_object.GetType().GetProperties().Where(
                p => Attribute.IsDefined(p, typeof(SQL_Column))
                );

            foreach (var column in sql_columns)
            {
                sql_attr_column = (SQL_Column)Attribute.GetCustomAttribute(column, typeof(SQL_Column));

                row = this.this_data_table.NewRow();
                row["field"] = column.Name;
                row["value"] = sql_attr_column.column_default;

                this.this_data_table.Rows.Add(row);
            }
        }

        //table properties
        private void GetTableProperties()
        {
            //get database columns
            this.this_database_table = database.QueryFetch($"SELECT * FROM [{this.table_name}]");
            this.this_database_table.TableName = this.table_name;
        }

        private void ModelTableCompare()
        {
            List<string> database_fields = this.this_database_table.AsEnumerable()
                .Select(r => r.Field<string>("field"))
                .ToList();

            List<string> new_fields = this.this_data_table.AsEnumerable()
                .Select(r => r.Field<string>("field"))
                .Where<string>(f => !database_fields.Contains(f))
                .ToList();


        }

        private void CreateUpdateDatabase(bool create, DataTable model_table) 
        {
            if (create)
            {
                string query = $"CREATE TABLE {this.table_name} " +
                    $"(" +
                    "[field] VARCHAR(100) PRIMARY KEY, " +
                    "[value] VARCHAR(1000) " +
                    ")";

                this.database.QueryExecute(query);

                foreach (DataRow row in this.this_data_table.Rows)
                {
                    
                }
            }
            else
            {

            }
        }

        public void Save(T model)
        {

        }
    }
}
