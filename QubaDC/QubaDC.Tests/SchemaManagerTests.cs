using QubaDC.DatabaseObjects;
using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace QubaDC.Tests
{
    public class SchemaManagerTests : IClassFixture<MySqlDBFixture>, IDisposable
    {
        private string currentDatabase;

        private SchemaManager SchemaManager;

        public SchemaManagerTests(MySqlDBFixture fixture)
        {
            MySQLDataConnection con = fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler()
                        ,new SeparatedCRUDHandler()
                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedTests" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.Fixture = fixture;
            this.SchemaManager = c.SchemaManager;
        }

        public void Dispose()
        {
            System.Diagnostics.Debug.WriteLine("Disposing: " + currentDatabase);
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        public MySqlDBFixture Fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }

        [Fact]
        public void GetSchemaWithoutStoredSchemaReturnsEmptySchema()
        {
            this.QBDC.CreateSMOTrackingTableIfNeeded();
            System.Diagnostics.Debug.WriteLine("In: GetSchemaWithoutStoredSchemaReturnsEmptySchema");
            System.Diagnostics.Debug.WriteLine("CurrentDB: " + currentDatabase);
            var Schema = this.SchemaManager.GetCurrentSchema();
            Assert.Equal(1, Schema.ID);
            Assert.Null(Schema.SMO);
            Assert.NotNull(Schema);
            Assert.NotNull(Schema.TimeOfCreation);
        }
    }
}
