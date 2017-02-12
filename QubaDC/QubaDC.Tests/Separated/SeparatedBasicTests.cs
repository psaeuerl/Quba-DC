using QubaDC.Separated;
using QubaDC.Tests.xUnitExtension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class SeparatedBasicTests : IClassFixture<MySqlDBFixture>, IDisposable
    {
        private string currentDatabase;

        public SeparatedBasicTests(MySqlDBFixture fixture)
        {
            MySQLDataConnection con = fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler(),
                         new SeparatedCRUDHandler()
                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedTests" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.Fixture = fixture;
        }

        public MySqlDBFixture Fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }

        public void Dispose()
        {
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void EnsureWeStartFromEmptyDB()
        {
            var allTables = Fixture.DataConnection.GetAllTables();
            Assert.Equal(0, allTables.Count());
            QBDC.Init();
            var allTablesAfterInit = Fixture.DataConnection.GetAllTables();
            Assert.Equal(2, allTablesAfterInit.Count());

        }      
    }
}
