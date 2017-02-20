using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using System.Data;

namespace QubaDC.Tests
{
    public class MySQLCrudRendererTests : IClassFixture<MySqlDBFixture>
    {
        public MySqlDBFixture MySQLDB { get; private set; }
        public MySQLCrudRenderer MySQLCrudRenderer { get; private set; }

        public MySQLCrudRendererTests(MySqlDBFixture f)
        {
            this.MySQLDB = f;
            this.MySQLCrudRenderer = new MySQLCrudRenderer();
        }

        [Fact]
        public void RenderTimeStampWorks()
        {
            var now = DateTime.Now;
            String Statement = "SELECT " + MySQLCrudRenderer.SerializeDateTime(now) + " FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1";
            var result = MySQLDB.DataConnection.ExecuteQuery(Statement);
            var nowFromDb =result.Select().First().Field<DateTime>(0);
            Assert.Equal(now.Year, nowFromDb.Year);
            Assert.Equal(now.Month, nowFromDb.Month);
            Assert.Equal(now.Day, nowFromDb.Day);
            Assert.Equal(now.Hour, nowFromDb.Hour);
            Assert.Equal(now.Minute, nowFromDb.Minute);
            Assert.Equal(now.Second, nowFromDb.Second);
            Assert.Equal(now.Millisecond, nowFromDb.Millisecond);
        }
    }
}

