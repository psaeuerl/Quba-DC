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
    public class SeparatedQueryStoreSMOTests : IDisposable
    {
        private string currentDatabase;

        public SeparatedQueryStoreSMOTests()
        {
            this.fixture = new MySqlDBFixture();
            MySQLDataConnection con = fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler()
                         ,new SeparatedCRUDHandler()
                         , new SeparatedQSSelectHandler()
                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedQueryStoreTests_" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.fixture = fixture;
        }

        public MySqlDBFixture fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }
        public bool Succcess { get; private set; } = false;

        public void Dispose()
        {
            if(Succcess)
                this.fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void RenameTableWorks()
        {
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

            RenameTable rt = new RenameTable()
            {
                OldSchema = t.Schema,
                OldTableName = t.TableName,
                NewSchema = t.Schema,
                NewTableName = "new_basic_table"
            };
            QBDC.SMOHandler.HandleSMO(rt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            //check that new schema contains renamed table
            //check that new schema does not contain original table

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            Assert.IsType<RenameTable>(newSchemaInfo.SMO);
            Assert.True(newSchemaInfo.Schema.ContainsTable(rt.NewSchema, rt.NewTableName));
            Assert.False(newSchemaInfo.Schema.ContainsTable(rt.OldSchema, rt.OldTableName));
            Assert.Throws<InvalidOperationException>(() => newSchemaInfo.Schema.FindTable(rt.OldSchema, rt.OldTableName));
            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);
            this.Succcess = true;
        }


        [Fact]
        public void DropTableWorks()
        {
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

            
            DropTable dt = new DropTable()
            {
                Schema = t.Schema,
                TableName = t.TableName,             
            };
            QBDC.SMOHandler.HandleSMO(dt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            //check that new schema contains renamed table
            //check that new schema does not contain original table

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            Assert.False(newSchemaInfo.Schema.ContainsTable(dt.Schema, dt.TableName));

            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);
            this.Succcess = true;
        }

    }
}
