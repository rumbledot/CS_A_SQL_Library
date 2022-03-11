using System;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Primary_Key : Attribute
    {
        public bool is_auto_increment { get; set; }
        public SQL_Primary_Key(bool is_auto_increment = true)
        {
            this.is_auto_increment = is_auto_increment;
        }
    }
}
