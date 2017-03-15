using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class SeparatedQBDCFixture : MySqlDBFixture
    {
        public SeparatedQBDCFixture() : base()
        {
            QubaDCSystem c = new MySQLQubaDCSystem(
                this.DataConnection,
                  new SeparatedSMOHandler()
                  //,new SeparatedCRUDHandler()
                );
            this.QBDCSystem = c;
            //Create Empty Schema
            this.CreateEmptyDatabase("SeperatedTests");
            this.DataConnection.UseDatabase("SeperatedTests");
        }


        public QubaDCSystem QBDCSystem { get; private set; }

        public override void Dispose()
        {
            base.Dispose();
        }

    }
}
