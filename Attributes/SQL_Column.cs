using System;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Column : System.Attribute
    {
        private string _column_name;
        public string column_name
        {
            get { return _column_name; }
            set { _column_name = value; }
        }

        private Type _column_type;
        public Type column_type
        {
            get { return _column_type; }
            set { _column_type = value; }
        }

        private string _column_type_parameter;
        public string column_type_parameter
        {
            get { return _column_type_parameter; }
            set { _column_type_parameter = value; }
        }

        private bool _column_nullable;
        public bool column_nullable
        {
            get { return _column_nullable; }
            set { _column_nullable = value; }
        }

        private string _column_default;
        public string column_default
        {
            get { return _column_default; }
            set { _column_default = value; }
        }

        public SQL_Column()
        {
            this._column_name = "";
            this._column_type = null;
            this._column_type_parameter = "";
            this._column_nullable = true;
            this._column_default = "";
        }

        public SQL_Column(string column_type_parameter, string column_default, bool nullable = true)
        {
            this._column_name = "";
            this._column_type = null;
            this._column_type_parameter = column_type_parameter;
            this._column_nullable = nullable;
            this._column_default = column_default;
        }

        public SQL_Column(bool nullable)
        {
            this._column_name = "";
            this._column_type = null;
            this._column_type_parameter = "";
            this._column_nullable = nullable;
            this._column_default = "";
        }

        public SQL_Column(string column_name = "", Type column_type = null, string column_type_parameter = "", string column_default = "", bool nullable = true)
        {
            this._column_name = column_name;
            this._column_type = column_type;
            this._column_type_parameter = column_type_parameter;
            this._column_nullable = nullable;
            this._column_default = "";
        }
    }
}
