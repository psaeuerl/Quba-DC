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
                     Server = "localhost"
                 },
                  new SeparatedSMOHandler(),
                  new SeparatedCRUDHandler()
                );
            this.QBDCSystem = c;
            //What to do?
            //Delete Everything from the database
            //Init the System
        }

        internal void DropDatabaseIfExists(string Database)
        {
            this.QBDCSystem.DataConnection.ExecuteNonQuerySQL("DROP DATABASE" + Database);
        }

        public QubaDCSystem QBDCSystem { get; private set; }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
