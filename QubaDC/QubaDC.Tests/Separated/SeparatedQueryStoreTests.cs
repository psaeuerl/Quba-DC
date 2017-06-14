using QubaDC.CRUD;
using QubaDC.Separated;
using QubaDC.SMO;
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
        public void ReexecutingWorks()
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

            ////Insert 2-3 Rows
            InsertOperation c3 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "3", "'1asdf'");
            QBDC.CRUDHandler.HandleInsert(c3);

            InsertOperation c4 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "4", "'2asdf'");
            QBDC.CRUDHandler.HandleInsert(c4);


            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            AssertResults(result, result2);
        }

        private void AssertResults(QueryStoreSelectResult result, QueryStoreReexecuteResult result2)
        {
            Assert.Equal(result2.Hash, result.Hash);

            StringWriter wt = new StringWriter();
            result.Result.WriteXml(wt);
            wt.Flush();
            String r1Data = wt.ToString();

            StringWriter wt2 = new StringWriter();
            result2.Result.WriteXml(wt2);
            wt2.Flush();
            String r2Data = wt2.ToString();
            Assert.Equal(r1Data, r2Data);

        }
    }
}
