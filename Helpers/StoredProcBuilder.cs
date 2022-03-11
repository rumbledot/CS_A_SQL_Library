using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A_SQL_Library.Helpers
{
    public static class StoredProcBuilder
    {
        public static string Build_UpdateList(DataTable table_dt)
        {
            string columns = "";

            foreach (DataColumn col in table_dt.Columns)
            {
                if (columns.Length > 0) columns += "\n,";
                columns += $"[{col.ColumnName}]=@{col.ColumnName} ";
            }

            return columns;
        }

        public static string Build_InsertColumnList(DataTable table_dt)
        {
            string columns = "";

            foreach (DataColumn col in table_dt.Columns)
            {
                if (columns.Length > 0) columns += "\n,";
                columns += $"[{col.ColumnName}] ";
            }

            return columns;
        }

        public static string Build_InsertValueList(DataTable table_dt)
        {
            string columns = "";

            foreach (DataColumn col in table_dt.Columns)
            {
                if (columns.Length > 0) columns += "\n,";
                columns += $"@{col.ColumnName} ";
            }

            return columns;
        }
    }
}
