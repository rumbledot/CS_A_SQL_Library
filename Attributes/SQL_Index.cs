using System;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Index : Attribute
    {
        public bool is_clustered { get; set; }
        public SQL_Index(bool is_clustered = false)
        {
            this.is_clustered = is_clustered;
        }
    }
}
