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

        public TableMetadataManager MetaManager { get; internal set; }

        public abstract void Visit(AddColum addColum);
        public abstract void Visit(MergeTable mergeTable);
        public abstract void Visit(RenameTable renameTable);
        public abstract void Visit(RenameColumn renameColumn);
        public abstract void Visit(PartitionTable partitionTable);
        public abstract void Visit(JoinTable joinTable);
        public abstract void Visit(DropTable dropTable);
        public abstract void Visit(DecomposeTable decomposeTable);
        public abstract void Visit(CreateTable createTable);
        public abstract void Visit(CopyTable copyTable);
        public abstract void Visit(DropColumn dropColumn);


        public void HandleSMO(SchemaModificationOperator op)
        {
            op.Accept(this);
        }
    }
}
