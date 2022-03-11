using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using A_Common_Library.Data;
using A_SQL_Library.Models;

namespace A_SQL_Library
{
    public class Database_Procedure : IDisposable
    {
        private Database_Connection connection;

        public Database_Procedure(Database_Connection connection)
        {
            this.connection = connection;
        }

        public void CheckStoredProcedures()
        {

            using (SqlConnection conn = new SqlConnection(connection.connection_string))
            {
                //02-03-2021 Abraham
                //add stored procedures checker for exo database?
                //18-02-2021 Abraham
                //this will check, compare and install needed stored procedures in the database
                //every stored procedure saved as .sql file in SQL_Scripts folder inside application folder
                //with create keyword
                string install_directory = AppDomain.CurrentDomain.BaseDirectory;
                List<string> filenames = new List<string>();
                List<DateTime> filecreation = new List<DateTime>();
                Dictionary<string, DateTime> sql_files = new Dictionary<string, DateTime>();
                string query = "";
                string procedure_type = "";
                DataTable result = new DataTable();

                try
                {
                    conn.Open();

                    SqlCommand com = new SqlCommand();
                    com.Connection = conn;
                    com.CommandType = CommandType.Text;

                    DirectoryInfo directory = new DirectoryInfo($"{install_directory}SQL_Scripts{Path.DirectorySeparatorChar}");
                    var files = directory.GetFiles().AsEnumerable()
                        .Where(f => f.Extension.ToLower().Equals(".sql"))
                        .OrderByDescending(f => f.Name);

                    files.ToList()
                        .ForEach(f => {
                            sql_files.Add(Path.GetFileNameWithoutExtension(f.Name), f.LastWriteTime);
                        });                    

                    int index = 0;
                    DateTime sp_date;
                    DateTime file_date;
                    SqlDataAdapter adapter;
                    foreach (string filename in sql_files.Keys)
                    {
                        try
                        {
                            if (filename.ToLower().Equals("indexes") 
                                || filename.ToLower().Equals("tabletypes")) continue;

                            result = new DataTable();
                            
                            com.CommandText = $"SELECT * FROM sysobjects WHERE name='{filename}'";
                            com.ExecuteNonQuery();

                            adapter = new SqlDataAdapter(com);
                            adapter.Fill(result);

                            //check if we need to update/change it
                            if (result.NotEmpty())
                            {
                                //stored proc's date in DB
                                sp_date = Convert.ToDateTime(result.Rows[0]["crdate"]);
                                //file's date
                                file_date = Convert.ToDateTime(sql_files[filename]);

                                //assuming an update/change been made when the file is more recent
                                //then stored this script to database
                                if (file_date > sp_date)
                                {
                                    //Console.WriteLine("file date is recent than DB");
                                    try
                                    {
                                        //what to create?
                                        procedure_type = "";
                                        switch (filename.Substring(0, 2))
                                        {
                                            case "fn":
                                                procedure_type = "FUNCTION";
                                                break;
                                            case "sp":
                                                procedure_type = "PROCEDURE";
                                                break;
                                            case "tr":
                                                procedure_type = "TRIGGER";
                                                break;
                                            default: //doesn't make sense, skip this scripts
                                                continue;
                                        }

                                        //drop the existing ones
                                        com.CommandText = $"DROP {procedure_type} {filename}";
                                        com.ExecuteNonQuery();

                                        //save the more recent procedure from script file
                                        com.CommandText = File.ReadAllText($"{install_directory}SQL_Scripts{Path.DirectorySeparatorChar}{filename}.sql");
                                        com.ExecuteNonQuery();
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception($"STORED PROC INSTALL - Exception on executing script file \n {ex.Message} \n {ex.StackTrace}");
                                        //Console.WriteLine("Error " + ex.Message);
                                    }
                                }
                                index++;
                            }
                            else //this script file is new, lets store it to DB programables
                            {
                                com.CommandText = File.ReadAllText($"{install_directory}SQL_Scripts{Path.DirectorySeparatorChar}{filename}.sql");
                                com.ExecuteNonQuery();
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception($"STORED PROC INSTALL - Failed on script file {filename} \n {ex.Message} \n {ex.StackTrace}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception($"STORED PROC INSTALL - Failed to get files \n {ex.Message} \n {ex.StackTrace}");
                }
                finally
                {
                    install_directory = "";
                    filenames = null;
                    filecreation = null;
                    query = "";
                    result = null;

                    conn.Close();
                }
            }
        }

        public void CreateTableTypes() 
        {
            string install_directory = AppDomain.CurrentDomain.BaseDirectory;

            if (File.Exists($"{install_directory}SQL_Scripts{Path.DirectorySeparatorChar}tabletypes.sql"))
            {
                using (SqlConnection conn = new SqlConnection(connection.connection_string))
                {
                    conn.Open();

                    try
                    {
                        SqlCommand com = new SqlCommand();
                        com.Connection = conn;

                        com = new SqlCommand();
                        com.Connection = conn;
                        com.CommandText = File.ReadAllText($"{install_directory}SQL_Scripts{Path.DirectorySeparatorChar}tabletypes.sql");
                        com.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"TABLE TYPES INSTALL - Exception \n {ex.Message} \n {ex.StackTrace}");
                    }
                }
            }
        }

        #region IDisposable Implementation
        private bool disposed = false;

        ~Database_Procedure()
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
                else
                {
                    //Console.WriteLine("Implicit call: Dispose is called through finalization.");
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
