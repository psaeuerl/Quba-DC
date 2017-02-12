using QubaDC.Separated;
using QubaDC.Tests.xUnitExtension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class SeparatedBasicTests
    {
        private static QubaDCSystem GetConnection()
        {
            QubaDCSystem c = new MySQLQubaDCSystem(
                 new MySQLDataConnection()
                 {
                     Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                     Server = "localhost"
                 },
                  new SeparatedSMOHandler(),
                  new SeparatedCRUDHandler()
                );
            return c;
        }

        [Fact, Order(1)]
        public void xyz()
        {
            Assert.True(true);
        }

        [Fact, Order(2)]
        public void xyz2()
        {
            Assert.True(true);
        }
    }
}
