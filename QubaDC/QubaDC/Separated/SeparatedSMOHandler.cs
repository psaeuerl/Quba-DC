using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;

namespace QubaDC.Separated
{
    public class SeparatedSMOHandler : SMOVisitor
    {
        internal override void Visit(RenameTable renameTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(PartitionTable partitionTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DropTable dropTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(CreateTable createTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(CopyTable copyTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DecomposeTable decomposeTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(JoinTable joinTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(RenameColumn renameColumn)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(MergeTable mergeTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(AddColum addColum)
        {
            throw new NotImplementedException();
        }
    }
}
