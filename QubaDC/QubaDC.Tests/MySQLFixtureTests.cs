using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class MySQLFixtureTests 
    {
        [Fact]
        public void DropDataBaseWorks()
        {
            SeparatedQBDCFixture f = new SeparatedQBDCFixture();
            f.DropDatabaseIfExists("non_existing");
            Assert.True(true);        
        }

        [Fact]
        public void CreateDataBaseWorks()
        {
            SeparatedQBDCFixture f = new SeparatedQBDCFixture();
            f.CreateEmptyDatabase("empty_DB");
            Assert.True(true);
        }


    }
}
