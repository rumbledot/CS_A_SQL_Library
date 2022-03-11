using System;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Text : Attribute
    {
        public SQL_Text()
        {
            
        }
    }
}
