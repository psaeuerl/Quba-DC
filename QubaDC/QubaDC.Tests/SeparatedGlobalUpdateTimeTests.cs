using QubaDC.CRUD;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.DataBuilder;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class SeparatedGlobalUpdateTimeTests : IClassFixture<MySqlDBFixture>, IDisposable
    {
        private string currentDatabase;

        private SchemaManager SchemaManager;

        public SeparatedGlobalUpdateTimeTests(MySqlDBFixture fixture)
        {
            MySQLDataConnection con = fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler()
                        , new SeparatedCRUDHandler()
                        , new SeparatedQSSelectHandler()

                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedTests" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.Fixture = fixture;
            this.SchemaManager = c.SchemaManager;
        }


        public MySqlDBFixture Fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }



        public void Dispose()
        {
            System.Diagnostics.Debug.WriteLine("Disposing: " + currentDatabase);
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void QBDCInitCreatesGlobalTimeStampTable()
        {
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();
            Assert.True(tables.Select(x => x.Name.ToLowerInvariant()).Contains(QubaDCSystem.GlobalUpdateTableName.ToLowerInvariant()));
            var update = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();
            Assert.NotNull(update);
            Assert.Equal(1, update.ID);            
        }

        [Fact]
        public void SMOCreatesGlobalUpdate()
        {
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();
            var update1 = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();

            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);

            var update2 = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();
            Assert.Equal(2, update2.ID);
            Assert.True(update2.DateTime > update1.DateTime);
        }


        [Fact]
        public void InsertCreatesGlobalUpdate()
        {
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();
            var update1 = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();

            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);

            var update2 = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();

            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.Visit(c);
            var update3 = QBDC.GlobalUpdateTimeManager.GetLatestUpdate();

            Assert.Equal(3, update3.ID);
            Assert.True(update2.DateTime > update1.DateTime);
        }
    }
}
