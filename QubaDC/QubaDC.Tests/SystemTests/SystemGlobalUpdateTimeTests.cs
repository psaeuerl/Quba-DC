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
    public abstract class SystemGlobalUpdateTimeTests : IDisposable
    {
        private string currentDatabase;

        private SchemaManager SchemaManager;


        public abstract String BuildDataBaseName();


        public abstract QubaDCSystem BuildSystem();

        public SystemGlobalUpdateTimeTests()
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


        public MySqlDBFixture Fixture { get; set; }
        public QubaDCSystem QBDC { get; private set; }



        public void Dispose()
        {
            System.Diagnostics.Debug.WriteLine("Disposing: " + currentDatabase);
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }


        [Fact]
        public void InsertCreatesGlobalUpdate()
        { 
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();

            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);

            DateTime timeAfterCreateTable = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());

            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'John'","'Developer'");
            QBDC.CRUDHandler.HandleInsert(c);
            DateTime timeAfterInsert = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());

            Assert.True(timeAfterInsert > timeAfterCreateTable);
        }

        [Fact]
        public void DeleteCreatesGlobalUpdate()
        {
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();

            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            DateTime timeAfterCreateTable = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());

            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'John'", "'Developer'");
            QBDC.CRUDHandler.HandleInsert(c);
            DateTime timeAfterInsert = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());

            DeleteOperation c2 = CreateTableBuilder.GetBasicTableDelete(this.currentDatabase, "1", "'John'");
            QBDC.CRUDHandler.HandleDeletOperation(c2);
            DateTime timeAfterDelete = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());
            
            Assert.True(timeAfterInsert > timeAfterCreateTable);
            Assert.True(timeAfterDelete > timeAfterInsert);
        }

        [Fact]
        public void UpdateCreatesGlobalUpdate()
        {
            //Create Basic Table
            QBDC.Init();
            var tables = QBDC.DataConnection.GetAllTables();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            DateTime timeAfterCreateTable = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());

            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'John'", "'Developer'");
            QBDC.CRUDHandler.HandleInsert(c);
            DateTime timeAfterInsert = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());


            UpdateOperation c2 = CreateTableBuilder.GetBasicTableUpdate(this.currentDatabase, "1", "McJohn");
            QBDC.CRUDHandler.HandleUpdateOperation(c2);

            DateTime timeAfterUpdate = QBDC.GlobalUpdateTimeManager.GetLatestUpdate(t.ToTable());


            Assert.True(timeAfterInsert > timeAfterCreateTable);
            Assert.True(timeAfterUpdate > timeAfterInsert);
        }
    }
}
