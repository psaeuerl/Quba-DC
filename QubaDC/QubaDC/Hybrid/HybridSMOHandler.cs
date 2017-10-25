using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Separated.SMO;
using QubaDC.Hybrid.SMO;

namespace QubaDC.Hybrid
{
    public class HybridSMOHandler : SMOVisitor
    {

        public override void Visit(RenameTable renameTable)
        {            
            HybridRenameTableHandler h = new HybridRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(renameTable);            
        }

        public override void Visit(PartitionTable partitionTable)
        {            
            HybridPartitionTableHandler h = new HybridPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(partitionTable);
        }

        public override void Visit(DropTable dropTable)
        {            
            HybridDropTableHandler h = new HybridDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(dropTable);
        }

        public override void Visit(CreateTable createTable)
        {            
            HybridCreateTableHandler h = new HybridCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(createTable);
        }

        public override void Visit(DropColumn dropColumn)
        {
            HybridDropColumnHandler h = new HybridDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(dropColumn);
        }

        public override void Visit(CopyTable copyTable)
        {
            HybridCopyTableHandler h = new HybridCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(copyTable);
        }

        public override void Visit(DecomposeTable decomposeTable)
        {
            HybridDecomposeTableHandler h = new HybridDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(decomposeTable);
        }

        public override void Visit(JoinTable joinTable)
        {
            HybridJoinTableHandler h = new HybridJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(joinTable);
        }

        public override void Visit(RenameColumn renameColumn)
        {
            HybridRenameColumnHandler h = new HybridRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(renameColumn);            
        }

        public override void Visit(MergeTable mergeTable)
        {
            HybridMergeTableHandler h = new HybridMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(mergeTable);
        }

        public override void Visit(AddColum addColum)
        {            
            HybridAddColumnHandler h = new HybridAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(addColum);
        }
    }
}
