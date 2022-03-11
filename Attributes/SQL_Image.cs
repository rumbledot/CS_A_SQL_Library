using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A_SQL_Library.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQL_Image : Attribute
    {
        public SQL_Image()
        {
            
        }
    }
}
