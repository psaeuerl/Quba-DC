using QubaDC.DatabaseObjects;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.DataBuilder;
using QubaDC.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace QubaDC.Tests
{
    public class SchemaManagerTests :  IDisposable
    {
        private string currentDatabase;

        private SchemaManager SchemaManager;

        public SchemaManagerTests( )
        {
            this.Fixture = new MySqlDBFixture();
            MySQLDataConnection con = Fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler()
                        , new SeparatedCRUDHandler()
                                                 , new SeparatedQSSelectHandler()

                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedTests" + Guid.NewGuid().ToString().Replace("-", "");
            Fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
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

        [Fact]
        public void GetSchemaActiveAtWorks()
        {
            this.QBDC.CreateSMOTrackingTableIfNeeded();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);

            CreateTable t2 = CreateTableBuilder.BuildBasicTable(this.currentDatabase,"t2");
            QBDC.SMOHandler.HandleSMO(t2);


            var schemata = this.SchemaManager.GetAllSchemataOrderdByIdDescending();
            foreach(var schema in schemata)
            {
                SchemaInfo schemaat = this.SchemaManager.GetSchemaActiveAt(schema.TimeOfCreation);
                String schemaser = JsonSerializer.SerializeObject(schema);
                String schemaatset = JsonSerializer.SerializeObject(schemaat);
                Assert.Equal(schemaser, schemaatset);                
            }
        }
    }
}
