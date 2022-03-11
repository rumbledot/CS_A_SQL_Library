using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using A_Common_Library.Encryption;

namespace A_SQL_Library.Models
{
    public class Database_Connection
    {
        public string Database_Name { get; set; } = "";
        public string Server_Address { get; set; } = "";
        public string Database_User_ID { get; set; } = "";
        public string Encrypted_Password { get; set; } = "";

        [XmlIgnore]
        public bool Validated { get; set; } = false;

        [XmlIgnore]
        public string connection_string
        {
            get
            {
                return $"server={Server_Address};Database={Database_Name};User Id={Database_User_ID};Password={Unencrypted_Password}";
            }
        }

        [XmlIgnore]
        public string Unencrypted_Password
        {
            get
            {
                if (string.IsNullOrEmpty(this.Encrypted_Password)) return "";

                using (StandardEncryption legacy = new StandardEncryption()) 
                { 
                    return legacy.Decrypt_Legacy(this.Encrypted_Password); 
                }
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    this.Encrypted_Password = "";
                    return;
                }

                using (StandardEncryption legacy = new StandardEncryption())
                {
                    this.Encrypted_Password = legacy.Encrypt_Legacy(value);
                }
            }
        }
    }
}
