using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class SchemaManagerTests : IClassFixture<MySqlDBFixture>, IDisposable
    {
        private string currentDatabase;

        private SchemaManager SchemaManager;

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
        }

        public void Dispose()
        {
            this.Fixture.DropDatabaseIfExists(currentDatabase);
        }

        public MySqlDBFixture Fixture { get; private set; }

        [Fact]
        public void GetSchemaWithoutStoredSchemaReturnsEmptySchema()
        {
            var Schema = this.SchemaManager.GetCurrentSchema();
            Assert.NotNull(Schema);
            Assert.NotNull(Schema.Tables);
            Assert.Equal(0, Schema.Tables.Count());
        }
    }
}
