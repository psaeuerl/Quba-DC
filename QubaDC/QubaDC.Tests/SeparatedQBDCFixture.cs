using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class SeparatedQBDCFixture : IDisposable
    {
        public SeparatedQBDCFixture()
        {
            QubaDCSystem c = new QubaDCSystem(
                 new MySQLDataConnection()
                 {
                     Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                     Server = "localhost",
                      DataBase = "mysql"
                 },
                  new SeparatedSMOHandler(),
                  new SeparatedCRUDHandler()
                );
            this.QBDCSystem = c;
            //What to do?
            //Delete Everything from the database
            //Init the System
        }

        internal void CreateEmptyDatabase(string Database)
        {
            DropDatabaseIfExists(Database);
            this.QBDCSystem.DataConnection.ExecuteNonQuerySQL("CREATE DATABASE " + Database);
            ((MySQLDataConnection)this.QBDCSystem.DataConnection).UseDatabase(Database);
        }

        internal void DropDatabaseIfExists(string Database)
        {
            try
            {
                this.QBDCSystem.DataConnection.ExecuteNonQuerySQL("DROP DATABASE " + Database);
            } catch(InvalidOperationException ex)
            {
                var e = ex.InnerException.Message;
                if(!(e.Contains("Can't drop database '") &&e.Contains( "'; database doesn't exist")))
                {
                    throw ex;
                };
            }
        }

        public QubaDCSystem QBDCSystem { get; private set; }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
