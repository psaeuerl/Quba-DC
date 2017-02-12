using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class MySqlDBFixture : IDisposable
    {
        public MySqlDBFixture()
        {
            this.DataConnection = new MySQLDataConnection()
            {
                Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                Server = "localhost",
                DataBase = "mysql"
            };
        }

        public MySQLDataConnection DataConnection { get; private set; }


        internal void CreateEmptyDatabase(string Database)
        {
            DropDatabaseIfExists(Database);
            this.DataConnection.ExecuteNonQuerySQL("CREATE DATABASE " + Database);
            (this.DataConnection).UseDatabase(Database);
        }

        internal void DropDatabaseIfExists(string Database)
        {
            try
            {
                this.DataConnection.ExecuteNonQuerySQL("DROP DATABASE " + Database);
            }
            catch (InvalidOperationException ex)
            {
                var e = ex.InnerException.Message;
                if (!(e.Contains("Can't drop database '") && e.Contains("'; database doesn't exist")))
                {
                    throw ex;
                };
            }
        }

        public virtual void Dispose()
        {
            ;//NOP
        }
    }
}
