using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using A_Common_Library.String;
using A_SQL_Library.Models;

namespace A_SQL_Library
{
    public class Database_Client : IDisposable
    {
        private string db_server, db_name, db_user, db_password;
        private SqlConnection db_connection;

        public string database_prefix { get; set; } = "";

        private bool _connection_status = false;
        public bool connection_status
        {
            get
            {
                return this._connection_status;
            }
        }

        private bool _connection_data_complete = false;
        private string _connection_string;
        public string connection_string
        {
            get
            {
                return _connection_string;
            }
        }

        public Database_Client(Database_Connection connection_settings)
        {
            if (connection_settings == null
                || connection_settings.Server_Address.Equals("")
                || connection_settings.Database_Name.Equals("")
                || connection_settings.Database_User_ID.Equals("")
                || connection_settings.Unencrypted_Password.Equals("")) return;

            this.db_server = connection_settings.Server_Address;
            this.db_name = connection_settings.Database_Name;
            this.db_user = connection_settings.Database_User_ID;
            this.db_password = connection_settings.Unencrypted_Password;
            this._connection_string = connection_settings.connection_string;

            this._connection_status = connection_settings.Validated;

            this._connection_data_complete = true;
        }

        public Database_Client(string db_server, string db_name, string db_user, string db_password, bool try_test_and_create = false)
        {
            if (db_server.Equals("") && db_name.Equals("") && db_user.Equals("") && db_password.Equals(""))
            {
                throw new ArgumentException("Please review database connection data");
            }
            else
            {
                this.db_server = db_server;
                this.db_name = db_name;
                this.db_user = db_user;
                this.db_password = db_password;
                this._connection_string = $"server={db_server};database={db_name};uid={db_user};pwd={db_password}";

                _connection_data_complete = true;
            }

            if (try_test_and_create)
            {
                try
                {
                    this.TestAndCreateDatabase();
                }
                catch (Exception ex)
                {

                    throw new Exception($"Test and Create database failed \n {ex.Message}");
                }
            }
        }

        public bool TestConnection(string[] tables_to_verify = null)
        {
            //preset class variables
            this.db_connection = new SqlConnection();
            this._connection_status = false;
            //variables
            string tables_to_check = "";
            string query = "";
            bool is_success = false;

            if (!this._connection_data_complete) throw new Exception("Connection data incomplete");

            try
            {
                if (tables_to_verify == null || tables_to_verify.Length <= 0)
                {
                    query = $"SELECT database_id FROM sys.databases WHERE Name='{this.db_name}'";
                }
                else
                {

                    foreach (string table in tables_to_verify)
                    {
                        tables_to_check += tables_to_check.Equals("") ? $"'{table}'" : $",'{table}'";
                    }
                    query = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN({tables_to_check})";
                }

                using (this.db_connection = new SqlConnection(this.connection_string))
                {
                    try
                    {
                        db_connection.Open();
                        SqlCommand command = new SqlCommand(query, db_connection);
                        int count = (int)command.ExecuteScalar();

                        if (tables_to_verify == null)
                        {
                            this.db_connection = new SqlConnection(this.connection_string);
                            this._connection_status = true;
                            is_success = true;
                        }
                        if (tables_to_verify != null && count == tables_to_verify.Length)
                        {
                            this.db_connection = new SqlConnection(this.connection_string);
                            this._connection_status = true;
                            is_success = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Failed to verify database {ex.Message} - {query}");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to test database connection {ex.Message}");
            }

            return is_success;
        }

        public void TestAndCreateDatabase()
        {
            Database_Connection testDB_conn = new Database_Connection()
            {
                Server_Address = this.db_server,
                Database_Name = "master",
                Database_User_ID = this.db_user,
                Unencrypted_Password = this.db_password
            };
            Database_Connection userDB_conn = new Database_Connection()
            {
                Server_Address = this.db_server,
                Database_Name = this.db_name,
                Database_User_ID = this.db_name,
                Unencrypted_Password = this.db_password
            };

            using (Database_Client testDB = new Database_Client(testDB_conn))
            {
                try
                {
                    if (testDB.TestConnection())
                    {
                        Console.WriteLine("Server connection tested");

                        try
                        {
                            string query = $"SELECT COUNT(*) FROM master.dbo.sysdatabases WHERE name='{this.db_name}'";
                            int db_exists = testDB.QueryScalar(query, -1);

                            if (db_exists > 0)
                            {
                                Console.WriteLine("database exists");
                            }
                            else
                            {
                                try
                                {
                                    query = $"CREATE DATABASE [{this.db_name}]";
                                    testDB.QueryExecute(query);

                                    Console.WriteLine($"New empty [{this.db_name}] created");
                                }
                                catch (Exception ex)
                                {
                                    throw new Exception($"New empty [{this.db_name}] failed. Please contact your system admin. " + ex.Message);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Database check failed. Please contact your system admin. " + ex.Message);
                        }
                    }
                    else
                    {
                        throw new Exception("Database test failed");
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Database test error : " + ex.Message);
                }
            }

            using (Database_Client testDB = new Database_Client(userDB_conn))
            {
                try
                {
                    if (testDB.TestConnection())
                    {
                        this._connection_status = true;
                        Console.WriteLine($"Database [{this.db_name}] connected");
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception($"Database test failed \n {ex.Message} \n {ex.StackTrace}");
                }
            }
        }

        private void OpenConnection()
        {
            if (this.connection_status)
            {
                if (this.db_connection.State == ConnectionState.Open) this.db_connection.Close();
                this.db_connection.Open();
            }
            else
            {
                throw new Exception($"Connection status failed");
            }
        }

        private void CloseConnection()
        {
            if (this.connection_status)
            {
                if (this.db_connection.State == ConnectionState.Open) this.db_connection.Close();
            }
            else
            {
                throw new Exception($"Connection status failed");
            }
        }

        public void QueryExecute(string query)
        {
            if (query.Equals("")) throw new Exception("Please provide your query");

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to execute {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }
        }

        public void QueryExecute(string query, List<SqlParameter> sql_parameters)
        {
            if (query.Equals("") || sql_parameters == null || sql_parameters.Count <= 0) throw new Exception("Please review the parameters");

            DataTable result = new DataTable();

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();

                    command.Parameters.AddRange(sql_parameters.ToArray());

                    //foreach (SqlParameter parameter in sql_parameters)
                    //{
                    //    command.Parameters.Add(parameter);
                    //}

                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to execute \n {ex.Message} \n {ex.StackTrace}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }
        }

        public DataTable QueryFetch(string query)
        {
            if (query.Equals("")) throw new Exception("Please provide your query");

            DataTable result = new DataTable();

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();

                    SqlDataAdapter adapter = new SqlDataAdapter(command);
                    adapter.Fill(result);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to fetch {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        public DataTable QueryFetch(string[] column_names, string table_name)
        {
            if (table_name.Equals("") || column_names == null || column_names.Length <= 0) throw new Exception("Please provide your query");

            DataTable result = new DataTable();

            string columns = "";

            foreach (string column_name in column_names)
            {
                columns += $"[{column_name.Sanitize().ToUpper()}],";
            }
            columns = columns.Substring(0, columns.Length - 1);

            string query = $"SELECT {columns} FROM {table_name}";

            return result;
        }

        public DataTable QueryFetch(string query, List<SqlParameter> sql_parameters)
        {
            if (query.Equals("") || sql_parameters == null || sql_parameters.Count <= 0) throw new Exception("Please review the parameters");

            DataTable result = new DataTable();

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();

                    foreach (SqlParameter parameter in sql_parameters)
                    {
                        command.Parameters.Add(parameter);
                    }

                    SqlDataAdapter adapter = new SqlDataAdapter(command);
                    adapter.Fill(result);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to fetch : {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        public DataTable QueryFetchSP(string name)
        {
            if (name.Equals("")) throw new Exception("Please provide Stored Procedure name");

            DataTable result = new DataTable();

            using (SqlCommand command = new SqlCommand())
            {
                try
                {
                    this.OpenConnection();

                    command.Connection = this.db_connection;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = name;

                    SqlDataAdapter adapter = new SqlDataAdapter(command);
                    adapter.Fill(result);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to fetch : {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        public bool QueryUpdate(string query)
        {
            if (query.Equals("")) throw new Exception("Please provide your query");

            bool success = false;

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();
                    command.ExecuteNonQuery();

                    success = true;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to update : {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return success;
        }

        public bool QueryUpdate(string query, List<SqlParameter> sql_parameters)
        {
            if (query.Equals("") || sql_parameters == null || sql_parameters.Count <= 0) throw new Exception("Please review the parameters");

            bool success = false;

            using (SqlCommand command = new SqlCommand(query, this.db_connection))
            {
                try
                {
                    this.OpenConnection();

                    foreach (SqlParameter parameter in sql_parameters)
                    {
                        command.Parameters.Add(parameter);
                    }
                    command.ExecuteNonQuery();

                    success = true;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to update {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return success;
        }

        public bool QueryUpdateSP(string name, List<SqlParameter> sql_parameters)
        {
            if (name.Equals("") || sql_parameters == null || sql_parameters.Count <= 0) throw new Exception("Please provide Stored Procedure name");

            bool success = false;

            using (SqlCommand command = new SqlCommand())
            {
                try
                {
                    this.OpenConnection();
                    command.Connection = db_connection;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = name;
                    command.ExecuteNonQuery();

                    success = true;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to update {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return success;
        }

        public int QueryScalar(string query)
        {
            return this.QueryScalar(query, -1);
        }

        public int QueryScalar(string query, int def_value = -1)
        {
            if (query.Equals("")) throw new Exception("Please provide your query");

            int result = def_value;

            using (SqlCommand command = new SqlCommand(query, db_connection))
            {
                try
                {
                    this.OpenConnection();
                    var res = command.ExecuteScalar();
                    if (res != null)
                    {
                        result = (int)res;
                    }
                    else
                    {
                        result = def_value;
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to query scalar {ex.Message}");
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        #region IDisposable Implementation
        private bool disposed = false;

        ~Database_Client()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                //Console.WriteLine("This is the first call to Dispose. Necessary clean-up will be done!");

                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    //Console.WriteLine("Explicit call: Dispose is called by the user.");
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.

                // TODO: set large fields to null.

                disposed = true;
            }
            else
            {
                //Console.WriteLine("Dispose is called more than one time. No need to clean up!");
            }
        }
        #endregion
    }
}
