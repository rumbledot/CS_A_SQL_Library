using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using A_Common_Library.XML;
using A_SQL_Library.Models;

namespace A_SQL_Library.Settings
{
    internal class Settings_File_Service : IDisposable
    {
        public Connection_Settings_File settings { get; set; }

        private string application_folder;
        private string settings_filename;
        public string settings_file_fullname
        {
            get
            {
                return $"{this.application_folder}\\{this.settings_filename}";
            }
        }
        public bool file_loaded = false;

        public Settings_File_Service(string application_folder, string project_name = "")
        {
            this.application_folder = application_folder;
            this.settings_filename = $"{project_name}_Connection_Settings.xml";
        }

        public bool IsExists()
        {
            return File.Exists(this.settings_file_fullname);
        }

        public void Load()
        {
            using (XMLUtility utility = new XMLUtility())
            {
                try
                {
                    this.settings = utility.XMLFile_LoadToObject<Connection_Settings_File>(this.settings_file_fullname);
                    this.file_loaded = true;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to load settings : {ex.Message}");
                }
            }
        }

        public void Save()
        {
            using (XMLUtility utility = new XMLUtility())
            {
                try
                {
                    utility.XMLFile_SaveFromObject<Connection_Settings_File>(this.settings, this.settings_file_fullname);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Failed to save settings : {ex.Message}");
                }
            }
        }

        #region IDisposable Implementation
        private bool disposed = false;

        ~Settings_File_Service()
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
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
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
