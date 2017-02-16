using QubaDC.DatabaseObjects;
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

        private readonly ITestOutputHelper output;

        public SchemaManagerTests(MySqlDBFixture fixture)
        {
            MySQLDataConnection con = fixture.DataConnection.Clone();
          
            //Create Empty Schema
            this.currentDatabase = "SchemaManagerTests" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.Fixture = fixture;
            this.SchemaManager = new MySqlSchemaManager(con);
            con.ExecuteNonQuerySQL(SchemaManager.GetCreateSchemaStatement());

            System.Diagnostics.Debug.WriteLine("Using Database: " + currentDatabase);
        }

        public void Dispose()
        {
            System.Diagnostics.Debug.WriteLine("Disposing: " + currentDatabase);
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        public MySqlDBFixture Fixture { get; private set; }

        [Fact]
        public void GetSchemaWithoutStoredSchemaReturnsEmptySchema()
        {
            System.Diagnostics.Debug.WriteLine("In: GetSchemaWithoutStoredSchemaReturnsEmptySchema");
            System.Diagnostics.Debug.WriteLine("CurrentDB: " + currentDatabase);
            var Schema = this.SchemaManager.GetCurrentSchema();
            Assert.NotNull(Schema);
            Assert.Null(Schema.ID);
            Assert.Null(Schema.Schema);
        }

        [Fact]
        public void StoringSchemaReturnsStorageInfo()
        {
            System.Diagnostics.Debug.WriteLine("In: StoringSchemaReturnsStorageInfo");
            System.Diagnostics.Debug.WriteLine("CurrentDB: " + currentDatabase);
            var xy = new Schema();
            xy.AddTable(new Table("schema1", "table1", "column1"), new Table("schema1_hist", "table1", "column1"));
            this.SchemaManager.StoreSchema(xy);
            var x = this.SchemaManager.GetCurrentSchema();
            //Assert.NotNull(Schema);
            //Assert.NotNull(Schema.Tables);
            //Assert.Equal(0, Schema.Tables.Count());
        }
    }
}
