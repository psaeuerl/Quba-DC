using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.CustomAsserts;
using QubaDC.Tests.DataBuilder;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class HybridQueryStoreSMOTests :  SystemQueryStoreSMOTests
    {

        public HybridQueryStoreSMOTests() : base()
        {
            HybridQBDCFixture f = new HybridQBDCFixture();
            this.SeparatedFixture = f;
            this.Fixture = f;
            base.Init();
        }

        public HybridQBDCFixture SeparatedFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "Hybrid_" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return SeparatedFixture.QBDCSystem;

        }
    }
}
