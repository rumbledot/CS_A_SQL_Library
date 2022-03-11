using System;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Timestamp : Attribute
    {
        public SQL_Timestamp()
        {

        }
    }
}
