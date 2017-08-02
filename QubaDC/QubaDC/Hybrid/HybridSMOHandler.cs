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
            HybridRenameTableHandler h = new HybridRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(renameTable);            
        }

        internal override void Visit(PartitionTable partitionTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DropTable dropTable)
        {
            HybridDropTableHandler h = new HybridDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(dropTable);
        }

        internal override void Visit(CreateTable createTable)
        {
            HybridCreateTableHandler h = new HybridCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(createTable);
        }

        internal override void Visit(DropColumn dropColumn)
        {
            HybridDropColumnHandler h = new HybridDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(dropColumn);
        }

        internal override void Visit(CopyTable copyTable)
        {
            HybridCopyTableHandler h = new HybridCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(copyTable);
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
            HybridRenameColumnHandler h = new HybridRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(renameColumn);            
        }

        internal override void Visit(MergeTable mergeTable)
        {
            HybridMergeTableHandler h = new HybridMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(mergeTable);
        }

        internal override void Visit(AddColum addColum)
        {
            HybridAddColumnHandler h = new HybridAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(addColum);
        }
    }
}
