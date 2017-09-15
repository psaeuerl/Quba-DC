using QubaDC.CRUD;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.CustomAsserts;
using QubaDC.Tests.DataBuilder;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public abstract class SystemQueryStoreCRUDTests : IDisposable
    {
        private string currentDatabase;

        public SystemQueryStoreCRUDTests()
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




        public MySqlDBFixture Fixture { get; set; }
        public QubaDCSystem QBDC { get; private set; }
        public bool Succcess { get; private set; } = false;

        public void Dispose()
        {
            if(Succcess)
                this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void ReexecutingAfterInsertWorks()
        {
            throw new NotImplementedException("Needs review");
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s =  SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            ////Insert 2-3 Rows
            InsertOperation c3 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "3", "'1asdf'");
            QBDC.CRUDHandler.HandleInsert(c3);

            InsertOperation c4 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "4", "'2asdf'");
            QBDC.CRUDHandler.HandleInsert(c4);


            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);

            //Check that reexecuting select produces different hash (ensures that delete  was executed)

            var resultAfterInsert = QBDC.QueryStore.ExecuteSelect(s);

            Assert.NotEqual(resultAfterInsert.Hash, result.Hash);
            this.Succcess = true;
        }

        [Fact]
        public void ReexecutingAfterDeleteWorks()
        {
            throw new NotImplementedException("Needs review");
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s = SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            //Delete one row
            DeleteOperation c3 = CreateTableBuilder.GetBasicTableDelete(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleDeletOperation(c3);

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);
            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);

            //Check that reexecuting select produces different hash (ensures that delete  was executed)

            var resultAfterDelete = QBDC.QueryStore.ExecuteSelect(s);

            Assert.NotEqual(resultAfterDelete.Hash, result.Hash);

            this.Succcess = true;
        }

        [Fact]
        public void ReexecutingAfterUpdateWorks()
        {
            throw new NotImplementedException("Needs review");
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s = SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            //Update one row
            UpdateOperation c3 = CreateTableBuilder.GetBasicTableUpdate(this.currentDatabase, "1", "asdfxyz");
            QBDC.CRUDHandler.HandleUpdateOperation(c3);

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);
            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);

            //Check that reexecuting select produces different hash (ensures that delete  was executed)

            var resultAfterDelete = QBDC.QueryStore.ExecuteSelect(s);

            Assert.NotEqual(resultAfterDelete.Hash, result.Hash);

            this.Succcess = true;
        }

    }
}
