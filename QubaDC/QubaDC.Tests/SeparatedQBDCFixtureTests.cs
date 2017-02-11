using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class SeparatedQBDCFixtureTests 
    {
        [Fact]
        public void DropDataBaseWorks()
        {
            SeparatedQBDCFixture f = new SeparatedQBDCFixture();
            f.DropDatabaseIfExists("XYZ");            
        }
    }
}
