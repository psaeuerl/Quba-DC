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

        public String[] GetTriggersForTable(String Schema,String Table)
        {
            String baseSelect = @"select TRIGGER_NAME
from information_schema.triggers
where TRIGGER_SCHEMA = '{0}'
AND EVENT_OBJECT_TABLE = '{1}'";
            String query = String.Format(baseSelect, Schema, Table);
            var resultTable = this.DataConnection.ExecuteQuery(query);
            var resultNames = resultTable.Select().Select(x => x.ItemArray[0].ToString()).ToArray();
            return resultNames;

        }

        public virtual void Dispose()
        {
            ;//NOP
        }
    }
}
