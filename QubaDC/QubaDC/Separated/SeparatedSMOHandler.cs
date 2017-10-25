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
        public override void Visit(RenameTable renameTable)
        {
            SeparatedRenameTableHandler h = new SeparatedRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(renameTable);
        }

        public override void Visit(PartitionTable partitionTable)
        {
            SeperatedPartitionTableHandler h = new SeperatedPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(partitionTable);
        }

        public override void Visit(DropTable dropTable)
        {
            SeparatedDropTableHandler h = new SeparatedDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(dropTable);
        }

        public override void Visit(CreateTable createTable)
        {
            SeparatedCreateTableHandler h = new SeparatedCreateTableHandler(this.DataConnection,this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(createTable);
        }

        public override void Visit(CopyTable copyTable)
        {
            SeparatedCopyTableHandler h = new SeparatedCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(copyTable);
        }

        public override void Visit(DecomposeTable decomposeTable)
        {
            SeparatedDecomposeTableHandler h = new SeparatedDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(decomposeTable);
        }

        public override void Visit(JoinTable joinTable)
        {
            SeparatedJoinTableHandler h = new SeparatedJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(joinTable);
        }

        public override void Visit(RenameColumn renameColumn)
        {
            SeparatedRenameColumnHandler h = new SeparatedRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(renameColumn);
        }

        public override void Visit(MergeTable mergeTable)
        {
            SeparatedMergeTableHandler h = new SeparatedMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer
                , this.MetaManager
                );
            h.Handle(mergeTable);
        }

        public override void Visit(AddColum addColum)
        {
            SeparatedAddColumnHandler h = new SeparatedAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(addColum);
        }

        public override void Visit(DropColumn dropColumn)
        {
            SepearatedDropColumnHandler h = new SepearatedDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(dropColumn);            
        }
    }
}
