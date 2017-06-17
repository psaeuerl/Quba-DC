using QubaDC;
using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class MySQLQueryStoreTests
    {
        public MySQLQueryStoreTests()
        {
            this.MySQLDB = new MySqlDBFixture();
        }

        public MySqlDBFixture MySQLDB { get; private set; }

        [Fact]
        public void InitWorking()
        {
            MySQLDB.CreateEmptyDatabase("mysqlstoretests");
            MySqlQueryStore q = new MySqlQueryStore(MySQLDB.DataConnection, new SeparatedQSSelectHandler());
            q.Init();
            var tables = MySQLDB.DataConnection.GetAllTables();
            Assert.True(tables.Any(x => x.Name == QueryStore.QueryStoreTable.ToLowerInvariant()));
        }
    }
}
