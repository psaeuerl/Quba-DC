using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace QubaDC
{
    public class MySQLDataConnection : DataConnection
    {
        public NetworkCredential Credentials { get; set; } 
        public String Server { get; set; }
        public String Database { get; set; }

        public override void CheckConnection()
        {

            string connStr = String.Format("server={0};user id={1}; password={2}; database=mysql; pooling=false",
            Server,Credentials.UserName, Credentials.Password);
            MySqlConnection con = null;
            try
            {
                con = new MySqlConnection(connStr);
                con.Open();

            }
            finally
            {
                if (con != null)
                    con.Close();
            }          
        }
    }
}
