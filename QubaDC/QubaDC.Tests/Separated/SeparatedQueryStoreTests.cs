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

namespace QubaDC.Tests.Separated
{
    public class SeparatedQueryStoreTests : IClassFixture<MySqlDBFixture>, IDisposable
    {
        private string currentDatabase;

        public SeparatedQueryStoreTests(MySqlDBFixture fixture)
        {
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
            this.Fixture = fixture;
        }

        public MySqlDBFixture Fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }

        public void Dispose()
        {
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void CreateTableWorks()
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
            ////Insert 2-3 Rows
            ////Replay Request
            ////=> Result should be the same
        }

      
    }
}
