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

        internal override void Visit(RenameTable renameTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridRenameTableHandler h = new HybridRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(renameTable);            
        }

        internal override void Visit(PartitionTable partitionTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridPartitionTableHandler h = new HybridPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(partitionTable);
        }

        internal override void Visit(DropTable dropTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridDropTableHandler h = new HybridDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(dropTable);
        }

        internal override void Visit(CreateTable createTable)
        {            
            HybridCreateTableHandler h = new HybridCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(createTable);
        }

        internal override void Visit(DropColumn dropColumn)
        {
            throw new NotImplementedException("Not Implemented");
            HybridDropColumnHandler h = new HybridDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(dropColumn);
        }

        internal override void Visit(CopyTable copyTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridCopyTableHandler h = new HybridCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(copyTable);
        }

        internal override void Visit(DecomposeTable decomposeTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridDecomposeTableHandler h = new HybridDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(decomposeTable);
        }

        internal override void Visit(JoinTable joinTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridJoinTableHandler h = new HybridJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(joinTable);
        }

        internal override void Visit(RenameColumn renameColumn)
        {
            throw new NotImplementedException("Not Implemented");
            HybridRenameColumnHandler h = new HybridRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(renameColumn);            
        }

        internal override void Visit(MergeTable mergeTable)
        {
            throw new NotImplementedException("Not Implemented");
            HybridMergeTableHandler h = new HybridMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(mergeTable);
        }

        internal override void Visit(AddColum addColum)
        {
            throw new NotImplementedException("Not Implemented");
            HybridAddColumnHandler h = new HybridAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(addColum);
        }
    }
}
