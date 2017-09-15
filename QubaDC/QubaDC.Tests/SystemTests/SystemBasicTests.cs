using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.DataBuilder;
using QubaDC.Tests.xUnitExtension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.SystemTests
{
    public abstract class SystemBasicTests : IDisposable
    {
        private string currentDatabase;

        public SystemBasicTests()
        {
        }

        public void Init()
        {
            MySQLDataConnection con = Fixture.DataConnection;
            this.currentDatabase = BuildDataBaseName();
            Fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.QBDC = BuildSystem();
        }

        public abstract String BuildDataBaseName();


        public abstract QubaDCSystem BuildSystem();




        public MySqlDBFixture Fixture { get;  set; }
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

        [Fact]
        public void CreateTableWorks()
        {
            throw new NotImplementedException("Needs review");
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            var allTablesAfterCreateTable = Fixture.DataConnection.GetAllTables();
            Assert.Contains("basictable", allTablesAfterCreateTable.Select(x => x.Name));
            Assert.Contains("basictable_1", allTablesAfterCreateTable.Select(x => x.Name));
            var schemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            var schema = schemaInfo.Schema;
            Assert.Equal(1, schema.HistTables.Count());
            Assert.Equal(1, schema.Tables.Count());
        }


    }
}
