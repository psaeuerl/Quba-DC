using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Separated.SMO;

namespace QubaDC.Separated
{
    public class SeparatedSMOHandler : SMOVisitor
    {
        internal override void Visit(RenameTable renameTable)
        {
            SeparatedRenameTableHandler h = new SeparatedRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(renameTable);
        }

        internal override void Visit(PartitionTable partitionTable)
        {
            SeperatedPartitionTableHandler h = new SeperatedPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(partitionTable);
        }

        internal override void Visit(DropTable dropTable)
        {
            SeparatedDropTableHandler h = new SeparatedDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(dropTable);
        }

        internal override void Visit(CreateTable createTable)
        {
            SeparatedCreateTableHandler h = new SeparatedCreateTableHandler(this.DataConnection,this.SchemaManager, this.SMORenderer);
            h.Handle(createTable);
        }

        internal override void Visit(CopyTable copyTable)
        {
            SeparatedCopyTableHandler h = new SeparatedCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(copyTable);
        }

        internal override void Visit(DecomposeTable decomposeTable)
        {
            SeparatedDecomposeTableHandler h = new SeparatedDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(decomposeTable);
        }

        internal override void Visit(JoinTable joinTable)
        {
            SeparatedJoinTableHandler h = new SeparatedJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(joinTable);
        }

        internal override void Visit(RenameColumn renameColumn)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(MergeTable mergeTable)
        {
            SeparatedMergeTableHandler h = new SeparatedMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(mergeTable);
        }

        internal override void Visit(AddColum addColum)
        {
            throw new NotImplementedException();
        }
    }
}
