using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A_SQL_Library.Models
{
    /// <summary>
    /// Use to generate XML settings file
    /// </summary>
    public class Connection_Settings_File
    {
        public List<Database_Connection> Connections = new List<Database_Connection> 
        { 
            new Database_Connection()
        };

        public double Uptime_Limit { get; set; } = 24;
        public int Restart_Minute_TimeOut { get; set; } = 1;
        public double Check_Setup_Time { get; set; } = 2;
        public string PrefferedTimeZone { get; set; } = "New Zealand Standard Time";
    }
}
