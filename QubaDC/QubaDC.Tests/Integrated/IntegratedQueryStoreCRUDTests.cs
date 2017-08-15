using QubaDC.CRUD;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.CustomAsserts;
using QubaDC.Tests.DataBuilder;
using QubaDC.Tests.Integrated;
using QubaDC.Tests.Separated;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Integrated
{
    public class IntegratedQueryStoreCRUDTests : SystemQueryStoreCRUDTests
    {
        public IntegratedQueryStoreCRUDTests() : base()
        {
            IntegratedQBDCFixture f = new IntegratedQBDCFixture();
            this.SeparatedFixture = f;
            this.Fixture = f;
            base.Init();
        }

        public IntegratedQBDCFixture SeparatedFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "Integrated_" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return SeparatedFixture.QBDCSystem;

        }
  
    }
}
