using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class MySqlFixtureTests 
    {
        [Fact]
        public void DropDataBaseWorks()
        {
            MySqlDBFixture f = new MySqlDBFixture();
            f.DropDatabaseIfExists("non_existing");
            Assert.True(true);        
        }

        [Fact]
        public void CreateDataBaseWorks()
        {
            MySqlDBFixture f = new MySqlDBFixture();
            f.CreateEmptyDatabase("empty_DB");
            Assert.True(true);
        }

        [Fact]
        public void GetAllTablesSakilaWorks()
        {
            MySqlDBFixture f = new MySqlDBFixture();
            f.DataConnection.UseDatabase("sakila");
            var tables = f.DataConnection.GetAllTables();
            Assert.Equal(23,tables.Count());
        }


    }
}
