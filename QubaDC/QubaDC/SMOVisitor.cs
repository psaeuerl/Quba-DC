using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;

namespace QubaDC
{
    public abstract class SMOVisitor
    {
        public DataConnection DataConnection { get; internal set; }
        public SchemaManager SchemaManager { get; internal set; }

        public SMORenderer SMORenderer { get; internal set; }

        internal abstract void Visit(AddColum addColum);
        internal abstract void Visit(MergeTable mergeTable);
        internal abstract void Visit(RenameTable renameTable);
        internal abstract void Visit(RenameColumn renameColumn);
        internal abstract void Visit(PartitionTable partitionTable);
        internal abstract void Visit(JoinTable joinTable);
        internal abstract void Visit(DropTable dropTable);
        internal abstract void Visit(DecomposeTable decomposeTable);
        internal abstract void Visit(CreateTable createTable);
        internal abstract void Visit(CopyTable copyTable);
        internal abstract void Visit(DropColumn dropColumn);


        public void HandleSMO(SchemaModificationOperator op)
        {
            op.Accept(this);
        }
    }
}
