using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Separated.SMO;

namespace QubaDC.SimpleSystem
{
    public class SimpleSMOHandler : SMOVisitor
    {
        public override void Visit(RenameTable renameTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(PartitionTable partitionTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(DropTable dropTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(CreateTable createTable)
        {
            SimpleMySqlSMORenderer s = new SimpleMySqlSMORenderer();
            String create = s.RenderCreateTable(createTable);
            this.DataConnection.ExecuteNonQuerySQL(create);
        }

        public override void Visit(DropColumn dropColumn)
        {
            throw new NotImplementedException();
        }

        public override void Visit(CopyTable copyTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(DecomposeTable decomposeTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(JoinTable joinTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(RenameColumn renameColumn)
        {
            throw new NotImplementedException();
        }

        public override void Visit(MergeTable mergeTable)
        {
            throw new NotImplementedException();
        }

        public override void Visit(AddColum addColum)
        {
            throw new NotImplementedException();
        }
    }
}
