using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using A_SQL_Library.Models;

namespace A_SQL_Library.Settings
{
    public class Settings_File_Controller : IDisposable
    {
        private string application_folder = "";
        private string application_name = "VIS";

        public bool file_loaded = false;
        public List<Database_Connection> connections = new List<Database_Connection>();
        public double Uptime_Limit = 24;
        public int Restart_Minute_TimeOut = 1;
        public double Check_Setup_Time = 2;
        public string PrefferedTimeZone = "New Zealand Standard Time";

        public Settings_File_Controller(string application_folder, string application_name)
        {
            this.application_folder = application_folder;
            this.application_name = application_name;
        }

        public void Load()
        {
            using (Settings_File_Service loader = new Settings_File_Service(this.application_folder, this.application_name))
            {
                if (File.Exists(loader.settings_file_fullname))
                {
                    loader.Load();

                    this.connections = loader.settings.Connections;

                    this.Uptime_Limit = loader.settings.Uptime_Limit;
                    this.Restart_Minute_TimeOut = loader.settings.Restart_Minute_TimeOut;
                    this.Check_Setup_Time = loader.settings.Check_Setup_Time;
                    this.PrefferedTimeZone = loader.settings.PrefferedTimeZone;

                    this.file_loaded = true;
                }
                else
                {
                    this.file_loaded = false;
                }
            }
        }

        public void Save()
        {
            using (Settings_File_Service saver = new Settings_File_Service(this.application_folder, this.application_name))
            {
                if (File.Exists(saver.settings_file_fullname))
                {
                    File.Delete(saver.settings_file_fullname);
                }

                Connection_Settings_File settings = new Connection_Settings_File();

                settings.Connections = connections;

                settings.Uptime_Limit = this.Uptime_Limit;
                settings.Restart_Minute_TimeOut = this.Restart_Minute_TimeOut;
                settings.Check_Setup_Time = this.Check_Setup_Time;
                settings.PrefferedTimeZone = this.PrefferedTimeZone;

                saver.settings = settings;

                saver.Save();
            }
        }

        #region IDisposable Implementation
        private bool disposed = false;

        ~Settings_File_Controller()
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
