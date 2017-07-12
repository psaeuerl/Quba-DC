using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.DataBuilder;
using QubaDC.Tests.xUnitExtension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class SeparatedBasicTests : SystemTests.SystemBasicTests
    {

        public SeparatedBasicTests() : base()
        {
            SeparatedQBDCFixture f = new SeparatedQBDCFixture();
            this.SeparatedFixture = f;
            this.Fixture = f;
            base.Init();
        }

        public SeparatedQBDCFixture SeparatedFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "SeparatedTests" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return SeparatedFixture.QBDCSystem;
        }
    }
}
