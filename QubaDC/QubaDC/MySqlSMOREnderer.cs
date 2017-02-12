using QubaDC.CRUD;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySqlSMORenderer : SMORenderer
    {
        public override string RenderCreateTable(CreateTable ct)
        {
            throw new NotImplementedException();
        }
    }
}
